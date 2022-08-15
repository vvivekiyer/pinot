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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.AdaptiveServerSelector;


public class ServerRoutingStatsEntry {
  String _serverInstanceId;
  private final ReentrantReadWriteLock _serverLock;
  private final PinotConfiguration _config;

  // Fields related to number of in-flight requests.
  private volatile int _numInFlightRequests;
  private final ExponentialMovingAverage _inFlighRequestsEma;

  // Fields related to latency
  private final ExponentialMovingAverage _emaLatencyMs;

  // Hybrid selector
  private final int _hybridSelectorExponent;

  public ServerRoutingStatsEntry(String serverInstanceId, PinotConfiguration pinotConfig) {
    _serverInstanceId = serverInstanceId;
    _serverLock = new ReentrantReadWriteLock();
    _config = pinotConfig;

    double alpha = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_ALPHA, AdaptiveServerSelector.ALPHA);
    long autoDecayWindowMs = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS,
        AdaptiveServerSelector.AUTODECAY_WINDOW_MS);
    int warmupCount =
        _config.getProperty(AdaptiveServerSelector.CONFIG_OF_WARMUP_COUNT, AdaptiveServerSelector.WARMUP_COUNT);
    double avgInitializationVal = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL,
        AdaptiveServerSelector.AVG_INITIALIZATION_VAL);

    _inFlighRequestsEma = new ExponentialMovingAverage(alpha, autoDecayWindowMs, warmupCount, avgInitializationVal);
    _emaLatencyMs = new ExponentialMovingAverage(alpha, autoDecayWindowMs, warmupCount, avgInitializationVal);

    _hybridSelectorExponent = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_HYBRID_SELECTOR_EXPONENT,
        AdaptiveServerSelector.HYBRID_SELECTOR_EXPONENT);
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
    double estimatedQSize = _numInFlightRequests + _inFlighRequestsEma.getAverage();

    return Math.pow(estimatedQSize, _hybridSelectorExponent) * _emaLatencyMs.getAverage();
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
