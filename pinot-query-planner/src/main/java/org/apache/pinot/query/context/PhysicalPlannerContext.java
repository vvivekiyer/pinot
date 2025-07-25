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
package org.apache.pinot.query.context;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.physical.v2.DistHashFunction;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Per-query unique context dedicated for the physical planner.
 */
public class PhysicalPlannerContext {
  private final Supplier<Integer> _nodeIdGenerator = new Supplier<>() {
    private int _id = 0;

    @Override
    public Integer get() {
      return _id++;
    }
  };
  /**
   * This is hacky. When assigning workers to the leaf-stage we cache the instanceId to QueryServerInstance values.
   * This way we can continue to use instance IDs throughout planning and convert them back to QueryServerInstance
   * while working with the Dispatchable Plan.
   * TODO: We should not use this map and instead have a centralized place for instanceId to QueryServerInstance
   *   mapping.
   */
  private final Map<String, QueryServerInstance> _instanceIdToQueryServerInstance = new HashMap<>();
  @Nullable
  private final RoutingManager _routingManager;
  private final String _hostName;
  private final int _port;
  private final long _requestId;
  /**
   * Instance ID of the instance corresponding to this process.
   */
  private final String _instanceId;
  private final Map<String, String> _queryOptions;
  private final boolean _useLiteMode;
  private final boolean _runInBroker;
  private final boolean _useBrokerPruning;
  private final int _liteModeServerStageLimit;
  private final DistHashFunction _defaultHashFunction;

  /**
   * Used by controller when it needs to extract table names from the query.
   * TODO: Controller should only rely on SQL parser to extract table names.
   */
  public PhysicalPlannerContext() {
    _routingManager = null;
    _hostName = "";
    _port = 0;
    _requestId = 0;
    _instanceId = "";
    _queryOptions = Map.of();
    _useLiteMode = CommonConstants.Broker.DEFAULT_USE_LITE_MODE;
    _runInBroker = CommonConstants.Broker.DEFAULT_RUN_IN_BROKER;
    _useBrokerPruning = CommonConstants.Broker.DEFAULT_USE_BROKER_PRUNING;
    _liteModeServerStageLimit = CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_LIMIT;
    _defaultHashFunction = DistHashFunction.valueOf(KeySelector.DEFAULT_HASH_ALGORITHM.toUpperCase());
  }

  public PhysicalPlannerContext(RoutingManager routingManager, String hostName, int port, long requestId,
      String instanceId, Map<String, String> queryOptions) {
    this(routingManager, hostName, port, requestId, instanceId, queryOptions,
        CommonConstants.Broker.DEFAULT_USE_LITE_MODE, CommonConstants.Broker.DEFAULT_RUN_IN_BROKER,
        CommonConstants.Broker.DEFAULT_USE_BROKER_PRUNING, CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_LIMIT,
        KeySelector.DEFAULT_HASH_ALGORITHM);
  }

  public PhysicalPlannerContext(RoutingManager routingManager, String hostName, int port, long requestId,
      String instanceId, Map<String, String> queryOptions, boolean defaultUseLiteMode, boolean defaultRunInBroker,
      boolean defaultUseBrokerPruning, int defaultLiteModeLeafStageLimit, String defaultHashFunction) {
    _routingManager = routingManager;
    _hostName = hostName;
    _port = port;
    _requestId = requestId;
    _instanceId = instanceId;
    _queryOptions = queryOptions == null ? Map.of() : queryOptions;
    _useLiteMode = QueryOptionsUtils.isUseLiteMode(_queryOptions, defaultUseLiteMode);
    _runInBroker = QueryOptionsUtils.isRunInBroker(_queryOptions, defaultRunInBroker);
    _useBrokerPruning = QueryOptionsUtils.isUseBrokerPruning(_queryOptions, defaultUseBrokerPruning);
    _liteModeServerStageLimit = QueryOptionsUtils.getLiteModeServerStageLimit(_queryOptions,
        defaultLiteModeLeafStageLimit);
    _defaultHashFunction = DistHashFunction.valueOf(defaultHashFunction.toUpperCase());
    _instanceIdToQueryServerInstance.put(instanceId, getBrokerQueryServerInstance());
  }

  public Supplier<Integer> getNodeIdGenerator() {
    return _nodeIdGenerator;
  }

  public Map<String, QueryServerInstance> getInstanceIdToQueryServerInstance() {
    return _instanceIdToQueryServerInstance;
  }

  @Nullable
  public RoutingManager getRoutingManager() {
    return _routingManager;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  public boolean isUseLiteMode() {
    return _useLiteMode;
  }

  public boolean isRunInBroker() {
    return _runInBroker;
  }

  public boolean isUseBrokerPruning() {
    return _useBrokerPruning;
  }

  public int getLiteModeServerStageLimit() {
    return _liteModeServerStageLimit;
  }

  /**
   * Gets a random instance id from the registered instances in the context.
   * <p>
   *   <b>Important:</b> This method will always return a server instanceId, unless no server has yet been registered
   *   with the context, which could happen for queries which don't consist of any table-scans.
   * </p>
   */
  public String getRandomInstanceId() {
    Preconditions.checkState(!_instanceIdToQueryServerInstance.isEmpty(), "No instances present in context");
    if (_instanceIdToQueryServerInstance.size() == 1) {
      return _instanceIdToQueryServerInstance.keySet().iterator().next();
    }
    int numCandidates = _instanceIdToQueryServerInstance.size() - 1;
    Random random = ThreadLocalRandom.current();
    return _instanceIdToQueryServerInstance.keySet().stream().filter(instanceId -> !_instanceId.equals(instanceId))
        .collect(Collectors.toList()).get(numCandidates == 1 ? 0 : random.nextInt(numCandidates - 1));
  }

  public DistHashFunction getDefaultHashFunction() {
    return _defaultHashFunction;
  }

  private QueryServerInstance getBrokerQueryServerInstance() {
    return new QueryServerInstance(_instanceId, _hostName, _port, _port);
  }
}
