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
package org.apache.pinot.broker.routing.adaptiveserverselector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.utils.Pair;


public class NumInFlightReqSelector implements AdaptiveServerSelector {
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final Random _random;

  public NumInFlightReqSelector(ServerRoutingStatsManager serverRoutingStatsManager) {
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _random = new Random();
  }

  @Override
  public String select(List<String> serverCandidates) {
    String selectedServer = null;
    int minValue = Integer.MAX_VALUE;

    for (String server : serverCandidates) {
      Integer numInFlightRequests = _serverRoutingStatsManager.fetchServerNumInFlightRequests(server);

      // No stats for this server. That means this server hasn't received any queries yet.
      if (numInFlightRequests == null) {
        int randIdx = _random.nextInt(serverCandidates.size());
        selectedServer = serverCandidates.get(randIdx);
        break;
      }

      if (numInFlightRequests < minValue) {
        minValue = numInFlightRequests;
        selectedServer = server;
      }
    }

    return selectedServer;
  }

  @Override
  public List<Pair<String, Double>> fetchServerRanking() {
    List<Pair<String, Integer>> pairList = new ArrayList<>();
    List<Pair<String, Double>> newPairList = new ArrayList<>();

    pairList = _serverRoutingStatsManager.fetchServerNumInFlightRequests();


    for (Pair<String, Integer> p : pairList) {
      newPairList.add(new Pair(p.getFirst(), new Double(p.getSecond())));
    }

    Collections.shuffle(newPairList);
    Collections.sort(newPairList, new Comparator<Pair<String, Double>>() {
      @Override
      public int compare(Pair<String, Double> o1, Pair<String, Double> o2) {
        int val = Double.compare(o1.getSecond(), o2.getSecond());
        return val;
      }
    });

    return newPairList;

    /*
    List<String> serverRankList = new ArrayList<>();
    for (Pair<String, Integer> entry : pairList) {
      serverRankList.add(entry.getFirst());
    }
    return serverRankList; */
  }
}
