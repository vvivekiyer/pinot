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
package org.apache.pinot.core.data.manager.realtime;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamMessageDecoder;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.Fixtures;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


// TODO Re-write this test using the stream abstraction
public class RealtimeSegmentDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "RealtimeSegmentDataManagerTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int PARTITION_GROUP_ID = 0;
  private static final int SEQUENCE_ID = 945;
  private static final long SEG_TIME_MS = 98347869999L;
  private static final LLCSegmentName SEGMENT_NAME =
      new LLCSegmentName(RAW_TABLE_NAME, PARTITION_GROUP_ID, SEQUENCE_ID, SEG_TIME_MS);
  private static final String SEGMENT_NAME_STR = SEGMENT_NAME.getSegmentName();
  private static final long START_OFFSET_VALUE = 198L;
  private static final LongMsgOffset START_OFFSET = new LongMsgOffset(START_OFFSET_VALUE);

  private final Map<Integer, ConsumerCoordinator> _partitionGroupIdToConsumerCoordinatorMap =
      new ConcurrentHashMap<>();

  private static TableConfig createTableConfig()
      throws Exception {
    return Fixtures.createTableConfig(FakeStreamConsumerFactory.class.getName(),
        FakeStreamMessageDecoder.class.getName());
  }

  private RealtimeTableDataManager createTableDataManager(TableConfig tableConfig) {
    final String instanceId = "server-1";
    SegmentBuildTimeLeaseExtender.getOrCreate(instanceId, new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()),
        tableConfig.getTableName());
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.getInstanceId()).thenReturn(instanceId);
    when(tableDataManager.getSegmentLock(any())).thenReturn(mock(Lock.class));
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    when(tableDataManager.getStatsHistory()).thenReturn(statsHistory);
    when(tableDataManager.getConsumerDir()).thenReturn(TEMP_DIR.getAbsolutePath() + "/consumerDir");
    return tableDataManager;
  }

  private SegmentZKMetadata createZkMetadata() {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    segmentZKMetadata.setStartOffset(START_OFFSET.toString());
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    return segmentZKMetadata;
  }

  private FakeRealtimeSegmentDataManager createFakeSegmentManager()
      throws Exception {
    return createFakeSegmentManager(false, new TimeSupplier(), null, null, null);
  }

  private FakeRealtimeSegmentDataManager createFakeSegmentManager(boolean noUpsert, TimeSupplier timeSupplier,
      @Nullable String maxRows, @Nullable String maxDuration, @Nullable TableConfig tableConfig)
      throws Exception {
    SegmentZKMetadata segmentZKMetadata = createZkMetadata();
    if (tableConfig == null) {
      tableConfig = createTableConfig();
    }
    if (noUpsert) {
      tableConfig.setUpsertConfig(null);
    }
    if (maxRows != null) {
      tableConfig.getIndexingConfig().getStreamConfigs()
          .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, maxRows);
    }
    if (maxDuration != null) {
      tableConfig.getIndexingConfig().getStreamConfigs()
          .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, maxDuration);
    }
    if (tableConfig.getIngestionConfig() == null) {
      tableConfig.setIngestionConfig(new IngestionConfig());
    }
    tableConfig.getIngestionConfig().setRetryOnSegmentBuildPrecheckFailure(true);
    RealtimeTableDataManager tableDataManager = createTableDataManager(tableConfig);
    LLCSegmentName llcSegmentName = new LLCSegmentName(SEGMENT_NAME_STR);
    _partitionGroupIdToConsumerCoordinatorMap.putIfAbsent(PARTITION_GROUP_ID,
        new ConsumerCoordinator(false, tableDataManager));
    Schema schema = Fixtures.createSchema();
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    return new FakeRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, tableDataManager,
        new File(TEMP_DIR, REALTIME_TABLE_NAME).getAbsolutePath(), schema, llcSegmentName,
        _partitionGroupIdToConsumerCoordinatorMap, serverMetrics, timeSupplier);
  }

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));

    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentBuildTimeLeaseExtender.initExecutor();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentBuildTimeLeaseExtender.shutdownExecutor();
  }

  // Test that we are in HOLDING state as long as the controller responds HOLD to our segmentConsumed() message.
  // we should not consume when holding.
  @Test
  public void testHolding()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(endOffset.toString()));
    // And then never consume as long as we get a hold response, 100 times.
    for (int i = 0; i < 100; i++) {
      segmentDataManager._responses.add(response);
    }

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.close();
  }

  // Test that we go to commit when the controller responds commit after 2 holds.
  @Test
  public void testCommitAfterHold()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response holdResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testSegmentBuildException()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    segmentDataManager._responses.add(commitResponse);
    segmentDataManager._failSegmentBuild = true;

    consumer.run();
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertTrue(segmentDataManager._notifySegmentBuildFailedWithDeterministicErrorCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.ERROR);
    segmentDataManager.close();
  }

  // Test hold, catchup. hold, commit
  @Test
  public void testCommitAfterCatchup()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset firstOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    final LongMsgOffset catchupOffset = new LongMsgOffset(firstOffset.getOffset() + 10);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(firstOffset.toString()));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP)
            .withStreamPartitionMsgOffset(catchupOffset.toString()));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse1);
    segmentDataManager._responses.add(catchupResponse);
    segmentDataManager._responses.add(holdResponse2);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testCommitAfterCatchupWithPeriodOffset()
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    tableConfig.getIndexingConfig().getStreamConfigs().put(
        StreamConfigProperties.constructStreamProperty(StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA,
            "fakeStream"), "2d");
    FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(false, new TimeSupplier(), null, null, tableConfig);
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset firstOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    final LongMsgOffset catchupOffset = new LongMsgOffset(firstOffset.getOffset() + 10);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(firstOffset.toString()));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP)
            .withStreamPartitionMsgOffset(catchupOffset.toString()));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse1);
    segmentDataManager._responses.add(catchupResponse);
    segmentDataManager._responses.add(holdResponse2);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testCommitAfterCatchupWithTimestampOffset()
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    tableConfig.getIndexingConfig().getStreamConfigs().put(
        StreamConfigProperties.constructStreamProperty(StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA,
            "fakeStream"), Instant.now().toString());
    FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(false, new TimeSupplier(), null, null, tableConfig);
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset firstOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    final LongMsgOffset catchupOffset = new LongMsgOffset(firstOffset.getOffset() + 10);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(firstOffset.toString()));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP)
            .withStreamPartitionMsgOffset(catchupOffset.toString()));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse1);
    segmentDataManager._responses.add(catchupResponse);
    segmentDataManager._responses.add(holdResponse2);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testDiscarded()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response discardResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.DISCARD));
    segmentDataManager._responses.add(discardResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.DISCARDED);
    segmentDataManager.close();
  }

  @Test
  public void testRetained()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    segmentDataManager._consumeOffsets.add(endOffset);
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStreamPartitionMsgOffset(endOffset.toString())
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.KEEP);
    final SegmentCompletionProtocol.Response keepResponse = new SegmentCompletionProtocol.Response(params);
    segmentDataManager._responses.add(keepResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.RETAINED);
    segmentDataManager.close();
  }

  @Test
  public void testNotLeader()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER));
    // And then never consume as long as we get a Not leader response, 100 times.
    for (int i = 0; i < 100; i++) {
      segmentDataManager._responses.add(response);
    }

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.close();
  }

  @Test
  public void testConsumingException()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();

    segmentDataManager._throwExceptionFromConsume = true;
    segmentDataManager._postConsumeStoppedCalled = false;
    consumer.run();
    Assert.assertTrue(segmentDataManager._postConsumeStoppedCalled);
    segmentDataManager.close();
  }

  // Tests to go online from consuming state

  // If the state is COMMITTED or RETAINED, nothing to do
  // If discarded or error state, then downloadAndReplace the segment
  @Test
  public void testOnlineTransitionAfterStop()
      throws Exception {
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    final long finalOffsetValue = START_OFFSET_VALUE + 600;
    final LongMsgOffset finalOffset = new LongMsgOffset(finalOffsetValue);
    metadata.setEndOffset(finalOffset.toString());

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.COMMITTED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.RETAINED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.DISCARDED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.ERROR);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If holding, but we have overshot the expected final offset, the download and replace
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
      segmentDataManager.setCurrentOffset(finalOffsetValue + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If catching up, but we have overshot the expected final offset, the download and replace
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager.setCurrentOffset(finalOffsetValue + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If catching up, but we did not get to the final offset, then download and replace
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(new LongMsgOffset(finalOffsetValue - 1));
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // But then if we get to the exact offset, we get to build and replace, not download
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(finalOffset);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
    }

    // But then if we get to the exact offset, we download the segment because consumer semaphore was never acquired.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(finalOffset);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // Test downloadAndReplace is called after buildAndReplace fails.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._failSegmentBuildAndReplace = true;
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
      segmentDataManager.setCurrentOffset(finalOffsetValue);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
    }
  }

  @Test
  public void testEndCriteriaChecking()
      throws Exception {
    // test reaching max row limit
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsIndexed(Fixtures.MAX_ROWS_IN_SEGMENT - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsIndexed(Fixtures.MAX_ROWS_IN_SEGMENT);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(), SegmentCompletionProtocol.REASON_ROW_LIMIT);
    }
    // test reaching max time limit
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      // We should still get false because there is no messages fetched
      segmentDataManager._timeSupplier.add(Fixtures.MAX_TIME_FOR_SEGMENT_CLOSE_MS + 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      // Once there are messages fetched, and the time exceeds the extended hour, we should get true
      setHasMessagesFetched(segmentDataManager, true);
      segmentDataManager._timeSupplier.add(TimeUnit.HOURS.toMillis(1));
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(), SegmentCompletionProtocol.REASON_TIME_LIMIT);
    }
    // In catching up state, test reaching final offset
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // In catching up state, test reaching final offset ignoring time
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._timeSupplier.add(Fixtures.MAX_TIME_FOR_SEGMENT_CLOSE_MS);
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // When we go from consuming to online state, time and final offset matter.
    // Case 1. We have reached final offset.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._timeSupplier.add(1);
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      segmentDataManager.setConsumeEndTime(segmentDataManager._timeSupplier.get() + 10);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // Case 2. We have reached time limit.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      final long endTime = segmentDataManager._timeSupplier.get() + 10;
      segmentDataManager.setConsumeEndTime(endTime);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      segmentDataManager._timeSupplier.set(endTime - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager._timeSupplier.set(endTime);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }

    // test end criteria reached if any of the index cannot take more rows
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(false, new TimeSupplier(), null,
        null, null)) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());

      segmentDataManager.setIndexCapacityThresholdBreached(true);

      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(),
          SegmentCompletionProtocol.REASON_INDEX_CAPACITY_THRESHOLD_BREACHED);
    }
  }

  private void setHasMessagesFetched(FakeRealtimeSegmentDataManager segmentDataManager, boolean hasMessagesFetched)
      throws Exception {
    Field field = RealtimeSegmentDataManager.class.getDeclaredField("_hasMessagesFetched");
    field.setAccessible(true);
    field.set(segmentDataManager, hasMessagesFetched);
  }

  // If commit fails, make sure that we do not re-build the segment when we try to commit again.
  @Test
  public void testReuseOfBuiltSegment()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();

    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    SegmentCompletionProtocol.Response commitSuccess = new SegmentCompletionProtocol.Response(params);
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SegmentCompletionProtocol.Response commitFailed = new SegmentCompletionProtocol.Response(params);

    // Set up the responses so that we get a failed respnse first and then a success response.
    segmentDataManager._responses.add(commitFailed);
    segmentDataManager._responses.add(commitSuccess);
    final long leaseTime = 50000L;

    // The first time we invoke build, it should go ahead and build the segment.
    File segmentTarFile = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertNotNull(segmentTarFile);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit());
    Assert.assertTrue(segmentTarFile.exists());

    segmentDataManager._buildSegmentCalled = false;

    // This time around it should not build the segment.
    File segmentTarFile1 = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertEquals(segmentTarFile1, segmentTarFile);
    Assert.assertTrue(segmentTarFile.exists());
    Assert.assertTrue(segmentDataManager.invokeCommit());
    Assert.assertFalse(segmentTarFile.exists());
    segmentDataManager.close();
  }

  // If commit fails, and we still have the file, make sure that we remove the file when we go
  // online.
  @Test
  public void testFileRemovedDuringOnlineTransition()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();

    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SegmentCompletionProtocol.Response commitFailed = new SegmentCompletionProtocol.Response(params);

    // Set up the responses so that we get a failed response first and then a success response.
    segmentDataManager._responses.add(commitFailed);
    final long leaseTime = 50000L;
    final long finalOffset = START_OFFSET_VALUE + 600;
    segmentDataManager.setCurrentOffset(finalOffset);

    // We have set up commit to fail, so we should carry over the segment file.
    File segmentTarFile = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertNotNull(segmentTarFile);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit());
    Assert.assertTrue(segmentTarFile.exists());

    // Now let the segment go ONLINE from CONSUMING, and ensure that the file is removed.
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    metadata.setEndOffset(new LongMsgOffset(finalOffset).toString());
    segmentDataManager._stopWaitTimeMs = 0;
    segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.goOnlineFromConsuming(metadata);
    Assert.assertFalse(segmentTarFile.exists());
    segmentDataManager.close();
  }

  @Test
  public void testShutdownTableDataManagerWillNotShutdownLeaseExtenderExecutor()
      throws Exception {
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    when(instanceDataManagerConfig.getUpsertConfig()).thenReturn(new PinotConfiguration());
    when(instanceDataManagerConfig.getDedupConfig()).thenReturn(new PinotConfiguration());
    TableDataManagerProvider tableDataManagerProvider = new DefaultTableDataManagerProvider();
    tableDataManagerProvider.init(instanceDataManagerConfig, mock(HelixManager.class), new SegmentLocks(), null);
    TableConfig tableConfig = createTableConfig();
    Schema schema = Fixtures.createSchema();
    TableDataManager tableDataManager = tableDataManagerProvider.getTableDataManager(tableConfig, schema);
    tableDataManager.start();
    tableDataManager.shutDown();
    Assert.assertFalse(SegmentBuildTimeLeaseExtender.isExecutorShutdown());
  }

  @Test
  public void testShouldNotSkipUnfilteredMessagesIfNotIndexedAndTimeThresholdIsReached()
      throws Exception {
    final int segmentTimeThresholdMins = 10;
    TimeSupplier timeSupplier = new TimeSupplier() {
      @Override
      public Long get() {
        long now = System.currentTimeMillis();
        // now() is called once in the run() method, then once on setting consumeStartTime, once before each batch
        // reading and once for every row indexation
        if (_timeCheckCounter.incrementAndGet() <= FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS + 5) {
          return now;
        }
        // Exceed segment time threshold
        return now + TimeUnit.MINUTES.toMillis(segmentTimeThresholdMins + 1);
      }
    };
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(true, timeSupplier,
        String.valueOf(FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS * 2), segmentTimeThresholdMins + "m", null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);

      RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
      final LongMsgOffset endOffset =
          new LongMsgOffset(START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      segmentDataManager._consumeOffsets.add(endOffset);
      final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(
                  SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withStreamPartitionMsgOffset(endOffset.toString()));
      segmentDataManager._responses.add(response);

      consumer.run();

      // millis() is called first in run before consumption, then once on setting consumeStartTime, then once for
      // each batch and once for each message in the batch, then once more when metrics are updated after each batch
      // is processed and then 4 more times in run() after consume loop
      Assert.assertEquals(timeSupplier._timeCheckCounter.get(), FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS + 9);
      Assert.assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(),
          START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getNumDocsIndexed(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
    }
  }

  @Test
  public void testShouldNotSkipUnfilteredMessagesIfNotIndexedAndRowCountThresholdIsReached()
      throws Exception {
    final int segmentTimeThresholdMins = 10;
    TimeSupplier timeSupplier = new TimeSupplier();
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(true, timeSupplier,
        String.valueOf(FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS), segmentTimeThresholdMins + "m", null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);

      RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
      final LongMsgOffset endOffset =
          new LongMsgOffset(START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      segmentDataManager._consumeOffsets.add(endOffset);
      final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(
                  SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withStreamPartitionMsgOffset(endOffset.toString()));
      segmentDataManager._responses.add(response);

      consumer.run();

      // millis() is called first in run before consumption, then once on setting consumeStartTime, then once for
      // each batch and once for each message in the batch, then once for metrics updates and then 4 more times in
      // run() after consume loop
      Assert.assertEquals(timeSupplier._timeCheckCounter.get(), FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS + 7);
      Assert.assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(),
          START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getNumDocsIndexed(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
    }
  }

  private static class TimeSupplier implements Supplier<Long> {
    protected final AtomicInteger _timeCheckCounter = new AtomicInteger();
    protected long _timeNow = System.currentTimeMillis();

    @Override
    public Long get() {
      _timeCheckCounter.incrementAndGet();
      return _timeNow;
    }

    public void set(long millis) {
      _timeNow = millis;
    }

    public void add(long millis) {
      _timeNow += millis;
    }
  }

  // Implementing Closeable to make it easier to use in try-with-resources
  public static class FakeRealtimeSegmentDataManager extends RealtimeSegmentDataManager implements Closeable {

    public Field _state;
    public Field _shouldStop;
    public Field _stopReason;
    public Field _segmentBuildFailedWithDeterministicError;
    public boolean _failSegmentBuildAndReplace = false;
    private Field _streamMsgOffsetFactory;
    public LinkedList<LongMsgOffset> _consumeOffsets = new LinkedList<>();
    public LinkedList<SegmentCompletionProtocol.Response> _responses = new LinkedList<>();
    public boolean _commitSegmentCalled = false;
    public boolean _buildSegmentCalled = false;
    public boolean _failSegmentBuild = false;
    public boolean _buildAndReplaceCalled = false;
    public int _stopWaitTimeMs = 100;
    private boolean _downloadAndReplaceCalled = false;
    private boolean _notifySegmentBuildFailedWithDeterministicErrorCalled = false;
    public boolean _throwExceptionFromConsume = false;
    public boolean _postConsumeStoppedCalled = false;
    public Map<Integer, ConsumerCoordinator> _consumerCoordinatorMap;
    public boolean _stubConsumeLoop = true;
    private TimeSupplier _timeSupplier;
    private boolean _indexCapacityThresholdBreached;

    private static InstanceDataManagerConfig makeInstanceDataManagerConfig() {
      InstanceDataManagerConfig dataManagerConfig = mock(InstanceDataManagerConfig.class);
      when(dataManagerConfig.getReadMode()).thenReturn(null);
      when(dataManagerConfig.getAvgMultiValueCount()).thenReturn(null);
      when(dataManagerConfig.getSegmentFormatVersion()).thenReturn(null);
      when(dataManagerConfig.isRealtimeOffHeapAllocation()).thenReturn(false);
      when(dataManagerConfig.getConfig()).thenReturn(new PinotConfiguration());
      return dataManagerConfig;
    }

    public FakeRealtimeSegmentDataManager(SegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
        RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, Schema schema,
        LLCSegmentName llcSegmentName, Map<Integer, ConsumerCoordinator> consumerCoordinatorMap,
        ServerMetrics serverMetrics, TimeSupplier timeSupplier)
        throws Exception {
      super(segmentZKMetadata, tableConfig, realtimeTableDataManager, resourceDataDir,
          new IndexLoadingConfig(makeInstanceDataManagerConfig(), tableConfig), schema, llcSegmentName,
          consumerCoordinatorMap.get(llcSegmentName.getPartitionGroupId()), serverMetrics, null, null,
          () -> true);
      _state = RealtimeSegmentDataManager.class.getDeclaredField("_state");
      _state.setAccessible(true);
      _shouldStop = RealtimeSegmentDataManager.class.getDeclaredField("_shouldStop");
      _shouldStop.setAccessible(true);
      _stopReason = RealtimeSegmentDataManager.class.getDeclaredField("_stopReason");
      _stopReason.setAccessible(true);
      _segmentBuildFailedWithDeterministicError =
          RealtimeSegmentDataManager.class.getDeclaredField("_segmentBuildFailedWithDeterministicError");
      _segmentBuildFailedWithDeterministicError.setAccessible(true);
      _consumerCoordinatorMap = consumerCoordinatorMap;
      _streamMsgOffsetFactory = RealtimeSegmentDataManager.class.getDeclaredField("_streamPartitionMsgOffsetFactory");
      _streamMsgOffsetFactory.setAccessible(true);
      _streamMsgOffsetFactory.set(this, new LongMsgOffsetFactory());
      _timeSupplier = timeSupplier;
    }

    public String getStopReason() {
      try {
        return (String) _stopReason.get(this);
      } catch (Exception e) {
        Assert.fail();
      }
      return null;
    }

    public PartitionConsumer createPartitionConsumer() {
      return new PartitionConsumer();
    }

    public SegmentBuildDescriptor invokeBuildForCommit(long leaseTime)
        throws SegmentBuildFailureException {
      super.buildSegmentForCommit(leaseTime);
      return getSegmentBuildDescriptor();
    }

    public boolean invokeCommit()
        throws Exception {
      return super.commitSegment("dummyUrl");
    }

    private void terminateLoopIfNecessary() {
      if (_consumeOffsets.isEmpty() && _responses.isEmpty()) {
        setShouldStop();
      }
    }

    private void setShouldStop() {
      try {
        _shouldStop.set(this, true);
      } catch (Exception e) {
        Assert.fail();
      }
    }

    @Override
    public void startConsumption() {
      // Do nothing.
    }

    @Override
    protected boolean consumeLoop()
        throws Exception {
      if (_stubConsumeLoop) {
        if (_throwExceptionFromConsume) {
          throw new PermanentConsumerException(new Throwable("Offset out of range"));
        }
        setCurrentOffset(_consumeOffsets.remove().getOffset());
        terminateLoopIfNecessary();
        return true;
      }
      return super.consumeLoop();
    }

    @Override
    protected SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
      return _responses.remove();
    }

    @Override
    protected SegmentCompletionProtocol.Response commit(String controllerVipUrl) {
      return _responses.remove();
    }

    @Override
    protected void postStopConsumedMsg(String reason) {
      _postConsumeStoppedCalled = true;
    }

    @Override
    protected void notifySegmentBuildFailedWithDeterministicError() {
      _notifySegmentBuildFailedWithDeterministicErrorCalled = true;
    }


    // TODO: Some of the tests rely on specific number of calls to the `now()` method in the SegmentDataManager.
    // This is not a good coding practice and makes the code very fragile. This needs to be fixed.
    // Invoking now() in any part of RealtimeSegmentDataManager code will break the following tests:
    // 1. RealtimeSegmentDataManagerTest.testShouldNotSkipUnfilteredMessagesIfNotIndexedAndRowCountThresholdIsReached
    // 2. RealtimeSegmentDataManagerTest.testShouldNotSkipUnfilteredMessagesIfNotIndexedAndTimeThresholdIsReached
    @Override
    protected long now() {
      // now() is called in the constructor before _timeSupplier is set
      if (_timeSupplier == null) {
        return System.currentTimeMillis();
      }
      return _timeSupplier.get();
    }

    @Override
    protected void hold() {
      terminateLoopIfNecessary();
      _timeSupplier.add(5000L);
    }

    @Override
    protected boolean buildSegmentAndReplace() {
      terminateLoopIfNecessary();
      _buildAndReplaceCalled = true;
      return !_failSegmentBuildAndReplace;
    }

    @Override
    protected SegmentBuildDescriptor buildSegmentInternal(boolean forCommit) {
      terminateLoopIfNecessary();
      _buildSegmentCalled = true;
      if (_failSegmentBuild) {
        try {
          _segmentBuildFailedWithDeterministicError.set(this, true);
        } catch (Exception e) {
          Assert.fail();
        }
        return null;
      }
      if (!forCommit) {
        return new SegmentBuildDescriptor(null, null, getCurrentOffset(), 0, 0, -1);
      }
      File segmentTarFile = new File(new File(TEMP_DIR, REALTIME_TABLE_NAME), "segmentFile");
      try {
        segmentTarFile.createNewFile();
      } catch (IOException e) {
        Assert.fail("Could not create file " + segmentTarFile);
      }
      return new SegmentBuildDescriptor(segmentTarFile, null, getCurrentOffset(), 0, 0, -1);
    }

    @Override
    protected boolean commitSegment(String controllerVipUrl) {
      terminateLoopIfNecessary();
      _commitSegmentCalled = true;
      return true;
    }

    @Override
    protected void downloadSegmentAndReplace(SegmentZKMetadata metadata) {
      terminateLoopIfNecessary();
      _downloadAndReplaceCalled = true;
    }

    @Override
    public void stop() {
      setShouldStop();
      _timeSupplier.add(_stopWaitTimeMs);
    }

    public void setCurrentOffset(long offset) {
      setOffset(offset, "_currentOffset");
    }

    public void setConsumeEndTime(long endTime) {
      setLong(endTime, "_consumeEndTime");
    }

    public void setNumRowsConsumed(int numRows) {
      setInt(numRows, "_numRowsConsumed");
    }

    public void setNumRowsIndexed(int numRows) {
      setInt(numRows, "_numRowsIndexed");
    }

    public void setFinalOffset(long offset) {
      setOffset(offset, "_finalOffset");
    }

    @Override
    protected boolean canAddMore() {
      return !_indexCapacityThresholdBreached;
    }

    public void setIndexCapacityThresholdBreached(boolean indexCapacityThresholdBreached) {
      _indexCapacityThresholdBreached = indexCapacityThresholdBreached;
    }

    public boolean invokeEndCriteriaReached() {
      Method endCriteriaReached = null;
      try {
        endCriteriaReached = RealtimeSegmentDataManager.class.getDeclaredMethod("endCriteriaReached");
        endCriteriaReached.setAccessible(true);
        Boolean result = (Boolean) endCriteriaReached.invoke(this);
        return result;
      } catch (NoSuchMethodException e) {
        Assert.fail();
      } catch (InvocationTargetException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
      throw new RuntimeException("Cannot get here");
    }

    public void setSegmentMaxRowCount(int numRows) {
      setInt(numRows, "_segmentMaxRowCount");
    }

    private void setLong(long value, String fieldName) {
      try {
        Field field = RealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(this, value);
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    private void setOffset(long value, String fieldName) {
      try {
        Field field = RealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        StreamPartitionMsgOffset offset = (StreamPartitionMsgOffset) field.get(this);
//        if (offset == null) {
        field.set(this, new LongMsgOffset(value));
//        } else {
//          offset.setOffset(value);
//        }
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    private void setInt(int value, String fieldName) {
      try {
        Field field = RealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setInt(this, value);
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    @Override
    public void close() {
      offload();
      destroy();
    }
  }
}
