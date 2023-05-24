package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;


public class AggFunctionQueryContextContext {
  private boolean _isNullHandlingEnabled = false;
  private List<OrderByExpressionContext> _orderByExpressions = null;
  // TODO(Vivek): Set this limit to a reasonable value.
  private int _limit = 0;

  public AggFunctionQueryContextContext(QueryContext queryContext) {
    _isNullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _orderByExpressions = queryContext.getOrderByExpressions();
    _limit = queryContext.getLimit();
  }

  public AggFunctionQueryContextContext() {
  }

  public boolean isNullHandlingEnabled() {
    return _isNullHandlingEnabled;
  }

  public List<OrderByExpressionContext> getOrderByExpressions() {
    return _orderByExpressions;
  }

  public int getLimit() {
    return _limit;
  }
}
