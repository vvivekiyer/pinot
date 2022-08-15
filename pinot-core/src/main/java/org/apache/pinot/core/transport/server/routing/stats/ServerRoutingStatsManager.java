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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 *  {@code ServerRoutingStatsManager} manages the query routing stats for each server. The stats are maintained at
 *  the broker and are updated when a query is submitted to a server and when a server responds after processing a
 *  query.
 */
public class ServerRoutingStatsManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRoutingStatsManager.class);

  private final PinotConfiguration _config;
  private volatile boolean _isEnabled;
  private ConcurrentHashMap<String, ServerRoutingStatsEntry> _serverQueryStatsMap;
  private ExecutorService _executorService;

  public ServerRoutingStatsManager(PinotConfiguration pinotConfig) {
    _config = pinotConfig;
  }

  public void init() {
    _isEnabled = _config.getProperty(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLETION,
        CommonConstants.Broker.AdaptiveServerSelector.ENABLE_STATS_COLLECTION);
    if (!_isEnabled) {
      LOGGER.info("Server stats collection for Adaptive Server Selection is not enabled.");
      return;
    }

    LOGGER.info("Initializing ServerRoutingStatsManager for Adaptive Server Selection.");
    _executorService = Executors.newFixedThreadPool(2);
    _serverQueryStatsMap = new ConcurrentHashMap<>();
  }

  public boolean isEnabled() {
    return _isEnabled;
  }

  public void shutDown() {
    // As the stats are not persistent, shutdown need not wait for task termination.
    if (!_isEnabled) {
      return;
    }

    LOGGER.info("Shutting down ServerRoutingStatsManager.");
    _isEnabled = false;
    _executorService.shutdownNow();
  }

  public int getQSize() {
    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
    return tpe.getQueue().size();
  }

  public long getCompletedTaskCount() {
    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
    return tpe.getCompletedTaskCount();
  }

  /**
   * Called when a query is submitted to a server. Updates stats corresponding to query submission.
   */
  public void recordStatsForQuerySubmit(long requestId, String serverInstanceId) {
    if (!_isEnabled) {
      return;
    }

    // TODO: Track Executor qSize and alert if it crosses a threshold.
    _executorService.execute(() -> {
      try {
        updateQuerySubmitStats(serverInstanceId);
      } catch (Exception e) {
        LOGGER.error("Exception caught while updating stats. requestId={}, exception={}", requestId, e);
        e.printStackTrace();
      }
    });
  }

  /**
   * Called when a query response is received from the server. Updates stats related to query completion.
   */
  private void updateQuerySubmitStats(String serverInstanceId) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.computeIfAbsent(serverInstanceId,
        k -> new ServerRoutingStatsEntry(serverInstanceId, _config));

    try {
      stats.getServerWriteLock().lock();
      stats.updateNumInFlightRequestsForSubmit();
    } finally {
      stats.getServerWriteLock().unlock();
    }
  }

  public void recordStatsForQueryCompletion(long requestId, String serverInstanceId, int latency) {
    if (!_isEnabled) {
      return;
    }

    _executorService.execute(() -> {
      try {
        updateQueryCompletionStats(serverInstanceId, latency);
      } catch (Exception e) {
        LOGGER.error("Exception caught while updating stats. requestId={}, exception={}", requestId, e);
        e.printStackTrace();
      }
    });
  }

  private void updateQueryCompletionStats(String serverInstanceId, int latencyMs) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.computeIfAbsent(serverInstanceId,
        k -> new ServerRoutingStatsEntry(serverInstanceId, _config));

    try {
      stats.getServerWriteLock().lock();

      stats.updateNumInFlightRequestsForResponse();
      if (latencyMs >= 0.0) {
        stats.updateLatencyForResponse(latencyMs);
      }
    } finally {
      stats.getServerWriteLock().unlock();
    }
  }

  /**
   * Returns an iterator to go over the stats for each server.
   * @return
   */
  public Iterator<Map.Entry<String, ServerRoutingStatsEntry>> getServerRoutingStatsItr() {
    return _serverQueryStatsMap.entrySet().iterator();
  }

  /**
   * Returns a list containing each server and the corresponding number of in-flight requests active on the server.
   */
  public List<Pair<String, Integer>> fetchServerNumInFlightRequests() {
    List<Pair<String, Integer>> response = new ArrayList<>();

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      int numInFlightRequests = stats.getNumInFlightRequests();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, numInFlightRequests));
    }

    return response;
  }

  /**
   * Same as above but returns the number of inflight requests for the input server.
   */
  public Integer fetchServerNumInFlightRequests(String server) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    stats.getServerReadLock().lock();
    int numInFlightRequests = stats.getNumInFlightRequests();
    stats.getServerReadLock().unlock();

    return numInFlightRequests;
  }

  /**
   * Returns a list containing each server and the corresponding EWMA latency seen for queries on the server.
   */
  public List<Pair<String, Double>> fetchServerLatency() {
    List<Pair<String, Double>> response = new ArrayList<>();

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double latency = stats.getEMALatency();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, latency));
    }

    return response;
  }

  /**
   * Same as above but returns the EWMA latency for the input server.
   */
  public Double fetchServerLatency(String server) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    stats.getServerReadLock().lock();
    double latency = stats.getEMALatency();
    stats.getServerReadLock().unlock();

    return latency;
  }

  /**
   * Returns a list containing each server and the corresponding score for each server. Used by HybridSelector.
   */
  public List<Pair<String, Double>> fetchServerScore() {
    List<Pair<String, Double>> response = new ArrayList<>();

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double score = stats.getServerScore();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, score));
    }

    return response;
  }

  /**
   * Same as above but returns the score for a single server.
   */
  public Double fetchServerScore(String server) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    stats.getServerReadLock().lock();
    double score = stats.getServerScore();
    stats.getServerReadLock().unlock();

    return score;
  }
}
