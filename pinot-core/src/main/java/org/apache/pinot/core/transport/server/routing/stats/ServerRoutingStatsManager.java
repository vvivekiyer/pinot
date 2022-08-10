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
import org.apache.pinot.spi.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerRoutingStatsManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRoutingStatsManager.class);
//  private final RateLimiter _queryLogRateLimiter = RateLimiter.create(100);

  private volatile boolean _isEnabled;
  private Map<String, ServerRoutingStatsEntry> _serverQueryStatsMap;
  private ExecutorService _executorService;

  public void init() {
    _isEnabled = true;
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
    _isEnabled = false;
    _executorService.shutdownNow();
  }

  // TODO(Vivek): Expand with other parameters.
  public void recordStatsForQuerySubmit(long requestId, String serverInstanceId) {
    Preconditions.checkState(_isEnabled, "ServerRoutingStatsManager is not enabled.");
//    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
//    LOGGER.info("Recording submit query stats for server={} : req={} : q_size={}", serverInstanceId, requestId,
//        tpe.getQueue().size());
//    try {
//      updateQuerySubmitStats(requestId, serverInstanceId);
//    } catch (Exception e) {
//      LOGGER.error("Exception caught while updating stats", e);
//      e.printStackTrace();
//    }


    _executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          updateQuerySubmitStats(requestId, serverInstanceId);
        } catch (Exception e) {
          LOGGER.error("Exception caught while updating stats", e);
          e.printStackTrace();
        }
      }
    });
  }

  // TODO(Vivek): Expand with other parameters.
  public void recordStatsForQueryCompletion(long requestId, String serverInstanceId, int latency) {
    Preconditions.checkState(_isEnabled, "ServerRoutingStatsManager is not enabled.");
//    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
//    LOGGER.info("Recording response query stats for server={} : req={} : q_size={}", serverInstanceId, requestId,
//        tpe.getQueue().size());
//    try {
//      updateQueryCompletionStats(requestId, serverInstanceId, latency);
//    } catch (Exception e) {
//      LOGGER.error("Exception caught while updating stats", e);
//      e.printStackTrace();
//    }


    _executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          updateQueryCompletionStats(requestId, serverInstanceId, latency);
        } catch (Exception e) {
          LOGGER.error("Exception caught while updating stats", e);
          e.printStackTrace();
        }
      }
    });
  }

  public Iterator<Map.Entry<String, ServerRoutingStatsEntry>> getServerRoutingStatsItr() {
    return _serverQueryStatsMap.entrySet().iterator();
  }

  private void updateQuerySubmitStats(long requestId, String serverInstanceId) {
    // LOGGER.info("Updating submit query stats for server={} : req={}", serverInstanceId, requestId);
    ServerRoutingStatsEntry stats =
        _serverQueryStatsMap.computeIfAbsent(serverInstanceId, k -> new ServerRoutingStatsEntry(serverInstanceId));

    try {
      stats.getServerWriteLock().lock();

      stats.updateNumInFlightRequestsForSubmit();
    } finally {
      stats.getServerWriteLock().unlock();
    }

    // LOGGER.info("NumInFlightRequests for server={} : val={}, req={}", serverInstanceId, val, requestId);
  }

  private void updateQueryCompletionStats(long requestId, String serverInstanceId, int latencyMs) {
//    LOGGER.info("Updating response query stats for server={} : req={}, latency={}", serverInstanceId, requestId,
//        latencyMs);

    ServerRoutingStatsEntry stats =
        _serverQueryStatsMap.computeIfAbsent(serverInstanceId, k -> new ServerRoutingStatsEntry(serverInstanceId));

    try {
      stats.getServerWriteLock().lock();

      stats.updateNumInFlightRequestsForResponse();
      if (latencyMs >= 0.0) {
        stats.updateLatencyForResponse(latencyMs);
      }
    } finally {
      stats.getServerWriteLock().unlock();
    }
//    LOGGER.info("Updated latency for server={} : req={}, latency={}", serverInstanceId, requestId, latency);
  }

  public List<Pair<String, Integer>> fetchServerNumInFlightRequests() {
    List<Pair<String, Integer>> response = new ArrayList<>();

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      int numInFlightRequests = stats.getNumInFlightRequests();
      stats.getServerReadLock().unlock();

      response.add(new Pair<>(server, numInFlightRequests));
    }

    return response;
  }

  public List<Pair<String, Double>> fetchServerLatency() {
    List<Pair<String, Double>> response = new ArrayList<>();

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double latency = stats.getEMALatency();
      stats.getServerReadLock().unlock();

      response.add(new Pair<>(server, latency));
    }

    return response;
  }

  public List<Pair<String, Double>> fetchServerScore() {
    List<Pair<String, Double>> response = new ArrayList<>();

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double score = stats.getServerScore();
      stats.getServerReadLock().unlock();

      response.add(new Pair<>(server, score));
    }

    return response;
  }


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
