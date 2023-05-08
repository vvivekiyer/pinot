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
package org.apache.pinot.query.runtime.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.IntermediateStageBlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory.getAggregationFunction;


/**
 *
 * AggregateOperator is used to aggregate values over a set of group by keys.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * Currently, we only support SUM/COUNT/MIN/MAX aggregation.
 *
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 * In this case, the input can be any type.
 *
 * If the list of aggregation calls is not empty, the input of aggregation has to be a number.
 *
 * Note: This class performs aggregation over the double value of input.
 * If the input is single value, the output type will be input type. Otherwise, the output type will be double.
 */
public class NewAggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";
  private static final Logger LOGGER = LoggerFactory.getLogger(NewAggregateOperator.class);

  private final MultiStageOperator _inputOperator;

  // TODO: Deal with the case where _aggCalls is empty but we have groupSet setup, which means this is a Distinct call.
  // TODO(Vivek): RexExpression will be converted to AggregationFunction[]


  private final DataSchema _resultSchema;
  private final AggregationUtils.Accumulator[] _accumulators;
  private final AggregationFunction[] _aggregationFunctions;
  private final AggregationResultHolder[] _aggregationResultHolders;
  private final GroupByResultHolder[] _groupByResultHolders;
  private final List<ExpressionContext> _groupSet;
  private final Map<Key, Object[]> _groupByKeyHolder;


  private TransferableBlock _upstreamErrorBlock;
  private boolean _readyToConstruct;
  private boolean _hasReturnedAggregateBlock;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  // TODO: Add these two checks when we confirm we can handle error in upstream ctor call.
  public NewAggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator, DataSchema dataSchema,
      List<FunctionContext> functionContexts, List<ExpressionContext> groupSet, DataSchema inputSchema, Object dummy) {
    super(context);
    _inputOperator = inputOperator;
    _groupSet = groupSet;
    _upstreamErrorBlock = null;

    _groupByKeyHolder = new HashMap<>();
    _accumulators = new AggregationUtils.Accumulator[functionContexts.size()];
    _aggregationFunctions = new AggregationFunction[functionContexts.size()];
    _aggregationResultHolders = new AggregationResultHolder[functionContexts.size()];
    _groupByResultHolders = new GroupByResultHolder[functionContexts.size()];
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      _aggregationFunctions[i] = getAggregationFunction(functionContexts.get(i), null);
      _aggregationResultHolders[i] = _aggregationFunctions[i].createAggregationResultHolder();
      // TODO(Vivek): Change it.
      _groupByResultHolders[i] = _aggregationFunctions[i].createGroupByResultHolder(10000, 1000);
    }


    _resultSchema = dataSchema;
    _readyToConstruct = false;
    _hasReturnedAggregateBlock = false;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_inputOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      if (!_readyToConstruct && !consumeInputBlocks()) {
        return TransferableBlockUtils.getNoOpTransferableBlock();
      }

      if (_upstreamErrorBlock != null) {
        return _upstreamErrorBlock;
      }

      if (!_hasReturnedAggregateBlock) {
        return produceAggregatedBlock();
      } else {
        // TODO: Move to close call.
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private TransferableBlock produceAggregatedBlock() {
    List<Object[]> rows = new ArrayList<>(_groupSet.size());
//    for (Map.Entry<Key, Object[]> e : _groupByKeyHolder.entrySet()) {
//      Object[] row = new Object[_aggCalls.size() + _groupSet.size()];
//      Object[] keyElements = e.getValue();
//      System.arraycopy(keyElements, 0, row, 0, keyElements.length);
//      for (int i = 0; i < _accumulators.length; i++) {
//        row[i + _groupSet.size()] = _accumulators[i].getResults().get(e.getKey());
//      }
//      rows.add(row);
//    }

    if (_groupSet.isEmpty()) {
      Object[] row = new Object[_aggregationFunctions.length];
      for (int i = 0; i < _aggregationFunctions.length; i++) {
        AggregationFunction aggregationFunction = _aggregationFunctions[i];
        row[i] = aggregationFunction.extractAggregationResult(_aggregationResultHolders[i]);
      }
      rows.add(row);
    } else {
      for (Map.Entry<Key, Object[]> e : _groupByKeyHolder.entrySet()) {
        Object[] row = new Object[_aggregationFunctions.length + _groupSet.size()];
        Object[] keyElements = e.getValue();
        System.arraycopy(keyElements, 0, row, 0, keyElements.length);
        for (int i = 0; i < _aggregationFunctions.length; i++) {
          row[i + _groupSet.size()] = _aggregationFunctions[i].extractGroupByResult(_groupByResultHolders[i],
              e.getKey().hashCode());
        }
        rows.add(row);
      }
    }

    _hasReturnedAggregateBlock = true;
    if (rows.size() == 0) {
      if (_groupSet.size() == 0) {
        return constructEmptyAggResultBlock();
      } else {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
    } else {
      return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
    }
  }

  /**
   * @return an empty agg result block for non-group-by aggregation.
   */
  private TransferableBlock constructEmptyAggResultBlock() {
    Object[] row = new Object[_aggregationFunctions.length];
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      row[i] = _accumulators[i].getMerger().initialize(null, _accumulators[i].getDataType());
    }
    return new TransferableBlock(Collections.singletonList(row), _resultSchema, DataBlock.Type.ROW);
  }

  /**
   * @return whether or not the operator is ready to move on (EOS or ERROR)
   */
  private boolean consumeInputBlocks() {
    TransferableBlock block = _inputOperator.nextBlock();
    while (!block.isNoOpBlock()) {
      // setting upstream error block
      if (block.isErrorBlock()) {
        _upstreamErrorBlock = block;
        return true;
      } else if (block.isEndOfStreamBlock()) {
        _readyToConstruct = true;
        return true;
      }

      List<Object[]> container = block.getContainer();

      // TODO(Vivek): Convert row to column representation only for the columns used in the aggregation function.
      // Convert row to columnar representation
      Map<Integer, List<Object>> columnValuesMap = new HashMap<>();
      for (Object[] row : container) {
        for (int i = 0; i < row.length; i++) {
          if (!columnValuesMap.containsKey(i)) {
            columnValuesMap.put(i, new ArrayList<>());
          }

          columnValuesMap.get(i).add(row[i]);
        }
      }


      if (_groupSet.isEmpty()) {
        // Simple aggregation function.
        PerformSimpleAggregation(container.size(), columnValuesMap);
      } else {
        // GroupBy with aggregation
        Key[] keys = GenerateGroupByKeys(container);
        PerformGroupByAggregation(container.size(), columnValuesMap, keys);
      }

      block = _inputOperator.nextBlock();
    }
    return false;
  }

  private Key[] GenerateGroupByKeys(List<Object[]> rows) {
    Key[] rowKeys = new Key[rows.size()];

    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);

      Object[] keyElements = new Object[_groupSet.size()];
      for (int j = 0; j < _groupSet.size(); j++) {
        keyElements[j] = row[_groupSet.get(j).getIdentifierIndex()];
      }

      Key rowKey = new Key(keyElements);
      _groupByKeyHolder.put(rowKey, rowKey.getValues());
      rowKeys[i] = rowKey;
    }

    return rowKeys;
  }

  private void PerformSimpleAggregation(int length, Map<Integer, List<Object>> columnValuesMap) {
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      aggregationFunction.aggregate(length, _aggregationResultHolders[i],
          getBlockValSetMap(aggregationFunction, columnValuesMap));
    }
  }

  private void PerformGroupByAggregation(int length, Map<Integer, List<Object>> columnValuesMap, Key[] keys) {

    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap = getBlockValSetMap(aggregationFunction, columnValuesMap);
      GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
      int[] intKeys = new int[length];
      for (int j = 0; j < length; j++) {
        intKeys[j] = keys[j].hashCode();
      }

      aggregationFunction.aggregateGroupBySV(length, intKeys, groupByResultHolder, blockValSetMap);
    }
  }

  private Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction aggregationFunction,
      Map<Integer, List<Object>> values) {
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    if (numExpressions == 1) {
      ExpressionContext expression = expressions.get(0);
      Preconditions.checkState(expression.getType().equals(ExpressionContext.Type.IDENTIFIER));

      List<Object> val = values.get(expression.getIdentifierIndex());
      return Collections.singletonMap(expression, new IntermediateStageBlockValSet(expression.getIdentifierDataType(), val));
    }
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    for (ExpressionContext expression : expressions) {
      Preconditions.checkState(expression.getType().equals(ExpressionContext.Type.IDENTIFIER));

      List<Object> val = values.get(expression.getIdentifierIndex());
      blockValSetMap.put(expression, new IntermediateStageBlockValSet(expression.getIdentifierDataType(), val));
    }
    return blockValSetMap;
  }
}
