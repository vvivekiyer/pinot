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
package org.apache.pinot.core.transport.server.routing.stats;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.common.utils.ExponentialMovingAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerRoutingStatsEntry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRoutingStatsEntry.class);

  String _serverInstanceId;
  private final ReentrantReadWriteLock _serverLock;

  // Fields related to number of in-flight requests.
  private volatile int _numInFlightRequests;
  private final ExponentialMovingAverage _inFlighRequestsEma;

  // Fields related to latency
  private final ExponentialMovingAverage _emaLatencyMs;

  private static final double ALPHA = 0.3333;
  private static final long AUTO_DECAY_WINDOW_MS = 10 * 1000;
  private static final int WARMUP_COUNT = 1000;

  public ServerRoutingStatsEntry(String serverInstanceId) {
    _serverInstanceId = serverInstanceId;
    _serverLock = new ReentrantReadWriteLock();

    _inFlighRequestsEma = new ExponentialMovingAverage(ALPHA, AUTO_DECAY_WINDOW_MS, WARMUP_COUNT);
    _emaLatencyMs = new ExponentialMovingAverage(ALPHA, AUTO_DECAY_WINDOW_MS, WARMUP_COUNT);
  }

  public ReentrantReadWriteLock.ReadLock getServerReadLock() {
    return _serverLock.readLock();
  }

  public ReentrantReadWriteLock.WriteLock getServerWriteLock() {
    return _serverLock.writeLock();
  }

  public Integer getNumInFlightRequests() {
    return _numInFlightRequests;
  }

  public Double getEMALatency() {
    return _emaLatencyMs.getAverage();
  }

  public double getServerScore() {
//    LOGGER.info("server={}, numInFlighRequests={}, numInFlightEMA={}, Latency={}", _serverInstanceId,
//        _numInFlightRequests, _inFlighRequestsEma.getAverage(), _emaLatencyMs.getAverage());
    double estimatedQSize = _numInFlightRequests + _inFlighRequestsEma.getAverage();
    return Math.pow(estimatedQSize, 3) * _emaLatencyMs.getAverage();
  }

  public void updateNumInFlightRequestsForSubmit() {
    ++_numInFlightRequests;
    _inFlighRequestsEma.compute(_numInFlightRequests);
  }

  public void updateNumInFlightRequestsForResponse() {
    --_numInFlightRequests;
  }

  public void updateLatencyForResponse(double latencyMs) {
    _emaLatencyMs.compute(latencyMs);
  }
}
