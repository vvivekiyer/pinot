/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.mailbox.channel;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code MailboxContentObserver} is the content streaming observer used to receive mailbox content.
 *
 * <p>When the observer onNext() is called (e.g. when data packet has arrived at the receiving end), it puts the
 * mailbox content to the receiving mailbox buffer; response with the remaining buffer size of the receiving mailbox
 * to the sender side.
 */
public class MailboxContentObserver implements StreamObserver<MailboxContent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxContentObserver.class);

  private final MailboxService _mailboxService;
  private final StreamObserver<MailboxStatus> _responseObserver;

  private final List<ByteBuffer> _mailboxBuffers;
  private transient ReceivingMailbox _mailbox;

  public MailboxContentObserver(
    MailboxService mailboxService, String mailboxId, StreamObserver<MailboxStatus> responseObserver) {
    _mailboxService = mailboxService;
    _mailbox = StringUtils.isNotBlank(mailboxId) ? _mailboxService.getReceivingMailbox(mailboxId) : null;
    _responseObserver = responseObserver;
    _mailboxBuffers = new ArrayList<>();
  }

  @Override
  public void onNext(MailboxContent mailboxContent) {
    String mailboxId = mailboxContent.getMailboxId();
    if (_mailbox == null) {
      _mailbox = _mailboxService.getReceivingMailbox(mailboxId);
    }
    _mailboxBuffers.add(mailboxContent.getPayload().asReadOnlyByteBuffer());
    if (mailboxContent.getWaitForMore()) {
      return;
    }
    try {
      long timeoutMs = Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
      List<ByteBuffer> buffers = new ArrayList<>(_mailboxBuffers);
      _mailboxBuffers.clear();
      ReceivingMailbox.ReceivingMailboxStatus status = _mailbox.offerRaw(buffers, timeoutMs);
      switch (status) {
        case SUCCESS:
          _responseObserver.onNext(MailboxStatus.newBuilder().setMailboxId(mailboxId)
              .putMetadata(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY,
                  Integer.toString(_mailbox.getNumPendingBlocks())).build());
          break;
        case CANCELLED:
          LOGGER.warn("Mailbox: {} already cancelled from upstream", mailboxId);
          cancelStream();
          break;
        case FIRST_ERROR:
          return;
        case ERROR:
          LOGGER.warn("Mailbox: {} already errored out (received error block before)", mailboxId);
          cancelStream();
          break;
        case TIMEOUT:
          LOGGER.warn("Timed out adding block into mailbox: {} with timeout: {}ms", mailboxId, timeoutMs);
          cancelStream();
          break;
        case EARLY_TERMINATED:
          LOGGER.debug("Mailbox: {} has been early terminated", mailboxId);
          _responseObserver.onNext(MailboxStatus.newBuilder().setMailboxId(mailboxId)
              .putMetadata(ChannelUtils.MAILBOX_METADATA_REQUEST_EARLY_TERMINATE, "true").build());
          break;
        default:
          throw new IllegalStateException("Unsupported mailbox status: " + status);
      }
    } catch (Exception e) {
      String errorMessage = "Caught exception while processing blocks for mailbox: " + mailboxId;
      LOGGER.error(errorMessage, e);
      _mailbox.setErrorBlock(
          ErrorMseBlock.fromException(new RuntimeException(errorMessage, e)), Collections.emptyList());
      cancelStream();
    }
  }

  private void cancelStream() {
    try {
      // NOTE: DO NOT use onError() because it will terminate the stream, and sender might not get the callback
      _responseObserver.onCompleted();
    } catch (Exception e) {
      // Exception can be thrown if the stream is already closed, so we simply ignore it
      LOGGER.debug("Caught exception cancelling mailbox: {}", _mailbox != null ? _mailbox.getId() : "unknown", e);
    }
  }

  @Override
  public void onError(Throwable t) {
    LOGGER.warn("Error on receiver side", t);
    if (_mailbox != null) {
      String msg = t != null ? t.getMessage() : "Unknown";
      _mailbox.setErrorBlock(ErrorMseBlock.fromError(
          QueryErrorCode.QUERY_CANCELLATION, "Cancelled by sender with exception: " + msg), List.of());
    } else {
      LOGGER.error("Got error before mailbox is set up", t);
    }
  }

  @Override
  public void onCompleted() {
    try {
      _responseObserver.onCompleted();
    } catch (Exception e) {
      // Exception can be thrown if the stream is already closed, so we simply ignore it
      LOGGER.debug("Caught exception sending complete to mailbox: {}", _mailbox != null ? _mailbox.getId() : "unknown",
          e);
    }
  }
}
