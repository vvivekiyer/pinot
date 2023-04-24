package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;


public class PinotSumMVAggregateFunction extends SqlAggFunction {
  public static final PinotSumMVAggregateFunction INSTANCE = new PinotSumMVAggregateFunction();

  private PinotSumMVAggregateFunction() {
    super("SUM_MV", null, SqlKind.OTHER_FUNCTION, ReturnTypes.AGG_SUM, null,
        OperandTypes.ARRAY, SqlFunctionCategory.NUMERIC, false, false, Optionality.FORBIDDEN);
  }
}
