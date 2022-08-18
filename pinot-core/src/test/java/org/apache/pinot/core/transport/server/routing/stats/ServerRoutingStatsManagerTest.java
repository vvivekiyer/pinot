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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ServerRoutingStatsManagerTest {
  @Test
  public void testInitAndShutDown() {
    Map<String, Object> properties = new HashMap<>();

    // Test 1: Test disabled.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLETION, false);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    assertFalse(manager.isEnabled());
    manager.init();
    assertFalse(manager.isEnabled());

    // Test 2: Test enabled.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLETION, true);
    manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    assertFalse(manager.isEnabled());
    manager.init();
    assertTrue(manager.isEnabled());

    // Test 3: Shutdown and then init.
    manager.shutDown();
    assertFalse(manager.isEnabled());

    manager.init();
    assertTrue(manager.isEnabled());
  }

  @Test
  public void testEmptyStats() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLETION, true);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    manager.init();

    Iterator<Map.Entry<String, ServerRoutingStatsEntry>> it = manager.getServerRoutingStatsItr();
    assertFalse(it.hasNext());

    List<Pair<String, Integer>> numInFlightReqList = manager.fetchServerNumInFlightRequests();
    assertTrue(numInFlightReqList.isEmpty());
    Integer numInFlightReq = manager.fetchServerNumInFlightRequests("testServer");
    assertNull(numInFlightReq);

    List<Pair<String, Double>> latencyList = manager.fetchServerLatency();
    assertTrue(latencyList.isEmpty());

    Double latency = manager.fetchServerLatency("testServer");
    assertNull(latency);

    List<Pair<String, Double>> scoreList = manager.fetchServerScore();
    assertTrue(scoreList.isEmpty());

    Double score = manager.fetchServerScore("testServer");
    assertNull(score);
  }

  @Test
  public void testQuerySubmitAndCompletionStats() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLETION, true);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ALPHA, 1.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, -1);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_COUNT, 0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL, 0.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_HYBRID_SELECTOR_EXPONENT, 3);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    manager.init();

    int requestId = 0;

    // Submit stats for server1.
    manager.recordStatsForQuerySubmit(requestId++, "server1");
    waitForStatsUpdate(manager, requestId);

    Iterator<Map.Entry<String, ServerRoutingStatsEntry>> it = manager.getServerRoutingStatsItr();
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server1");

    List<Pair<String, Integer>> numInFlightReqList = manager.fetchServerNumInFlightRequests();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 1);

    Integer numInFlightReq = manager.fetchServerNumInFlightRequests("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    List<Pair<String, Double>> latencyList = manager.fetchServerLatency();
    assertEquals(latencyList.get(0).getLeft(), "server1");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);

    Double latency = manager.fetchServerLatency("server1");
    assertEquals(latency, 0.0);

    List<Pair<String, Double>> scoreList = manager.fetchServerScore();
    assertEquals(scoreList.get(0).getLeft(), "server1");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);

    Double score = manager.fetchServerScore("server1");
    assertEquals(score, 0.0);

    // Submit more stats for server 1.
    manager.recordStatsForQuerySubmit(requestId++, "server1");
    waitForStatsUpdate(manager, requestId);

    it = manager.getServerRoutingStatsItr();
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server1");

    numInFlightReqList = manager.fetchServerNumInFlightRequests();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 2);

    numInFlightReq = manager.fetchServerNumInFlightRequests("server1");
    assertEquals(numInFlightReq.intValue(), 2);

    latencyList = manager.fetchServerLatency();
    assertEquals(latencyList.get(0).getLeft(), "server1");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);

    latency = manager.fetchServerLatency("server1");
    assertEquals(latency, 0.0);

    scoreList = manager.fetchServerScore();
    assertEquals(scoreList.get(0).getLeft(), "server1");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);

    score = manager.fetchServerScore("server1");
    assertEquals(score, 0.0);

    // Add a new server server2.
    manager.recordStatsForQuerySubmit(requestId++, "server2");
    waitForStatsUpdate(manager, requestId);

    it = manager.getServerRoutingStatsItr();
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server2");
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server1");

    numInFlightReqList = manager.fetchServerNumInFlightRequests();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 1);
    assertEquals(numInFlightReqList.get(1).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(1).getRight().intValue(), 2);

    numInFlightReq = manager.fetchServerNumInFlightRequests("server2");
    assertEquals(numInFlightReq.intValue(), 1);
    numInFlightReq = manager.fetchServerNumInFlightRequests("server1");
    assertEquals(numInFlightReq.intValue(), 2);

    latencyList = manager.fetchServerLatency();
    assertEquals(latencyList.get(0).getLeft(), "server2");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);
    assertEquals(latencyList.get(1).getLeft(), "server1");
    assertEquals(latencyList.get(1).getRight().doubleValue(), 0.0);

    latency = manager.fetchServerLatency("server2");
    assertEquals(latency, 0.0);
    latency = manager.fetchServerLatency("server1");
    assertEquals(latency, 0.0);

    scoreList = manager.fetchServerScore();
    assertEquals(scoreList.get(0).getLeft(), "server2");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);
    assertEquals(scoreList.get(1).getLeft(), "server1");
    assertEquals(scoreList.get(1).getRight().doubleValue(), 0.0);

    score = manager.fetchServerScore("server2");
    assertEquals(score, 0.0);
    score = manager.fetchServerScore("server1");
    assertEquals(score, 0.0);

    // Record completion stats for server1
    manager.recordStatsForQueryCompletion(requestId++, "server1", 2);
    waitForStatsUpdate(manager, requestId);

    it = manager.getServerRoutingStatsItr();
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server2");
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server1");

    numInFlightReqList = manager.fetchServerNumInFlightRequests();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 1);
    assertEquals(numInFlightReqList.get(1).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(1).getRight().intValue(), 1);

    numInFlightReq = manager.fetchServerNumInFlightRequests("server2");
    assertEquals(numInFlightReq.intValue(), 1);
    numInFlightReq = manager.fetchServerNumInFlightRequests("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    latencyList = manager.fetchServerLatency();
    assertEquals(latencyList.get(0).getLeft(), "server2");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);
    assertEquals(latencyList.get(1).getLeft(), "server1");
    assertEquals(latencyList.get(1).getRight().doubleValue(), 2.0);

    latency = manager.fetchServerLatency("server2");
    assertEquals(latency, 0.0);
    latency = manager.fetchServerLatency("server1");
    assertEquals(latency, 2.0);

    scoreList = manager.fetchServerScore();
    assertEquals(scoreList.get(0).getLeft(), "server2");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);
    assertEquals(scoreList.get(1).getLeft(), "server1");
    assertEquals(scoreList.get(1).getRight().doubleValue(), 54.0);

    score = manager.fetchServerScore("server2");
    assertEquals(score, 0.0);
    score = manager.fetchServerScore("server1");
    assertEquals(score, 54.0);

    // Record completion stats for server2
    manager.recordStatsForQueryCompletion(requestId++, "server2", 10);
    waitForStatsUpdate(manager, requestId);

    it = manager.getServerRoutingStatsItr();
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server2");
    assertTrue(it.hasNext());
    assertEquals(it.next().getKey(), "server1");

    numInFlightReqList = manager.fetchServerNumInFlightRequests();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 0);
    assertEquals(numInFlightReqList.get(1).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(1).getRight().intValue(), 1);

    numInFlightReq = manager.fetchServerNumInFlightRequests("server2");
    assertEquals(numInFlightReq.intValue(), 0);
    numInFlightReq = manager.fetchServerNumInFlightRequests("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    latencyList = manager.fetchServerLatency();
    assertEquals(latencyList.get(0).getLeft(), "server2");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 10.0);
    assertEquals(latencyList.get(1).getLeft(), "server1");
    assertEquals(latencyList.get(1).getRight().doubleValue(), 2.0);

    latency = manager.fetchServerLatency("server2");
    assertEquals(latency, 10.0);
    latency = manager.fetchServerLatency("server1");
    assertEquals(latency, 2.0);

    scoreList = manager.fetchServerScore();
    assertEquals(scoreList.get(0).getLeft(), "server2");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 10.0);
    assertEquals(scoreList.get(1).getLeft(), "server1");
    assertEquals(scoreList.get(1).getRight().doubleValue(), 54.0);

    score = manager.fetchServerScore("server2");
    assertEquals(score, 10.0);
    score = manager.fetchServerScore("server1");
    assertEquals(score, 54.0);
  }

  private void waitForStatsUpdate(ServerRoutingStatsManager serverRoutingStatsManager, long taskCount) {
    TestUtils.waitForCondition(aVoid -> {
      return (serverRoutingStatsManager.getCompletedTaskCount() == taskCount);
    }, 10L, 5000, "Failed to record stats for AdaptiveServerSelectorTest");
  }
}
