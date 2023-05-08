package org.apache.pinot.core.common;

import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public class IntermediateStageBlockValSet implements BlockValSet {
  private final FieldSpec.DataType _dataType;
  private final PinotDataType _pinotDataType;
  private final List<Object> _values;

  public IntermediateStageBlockValSet(DataSchema.ColumnDataType columnDataType, List<Object> values) {
    _dataType = columnDataType.toDataType();
    _pinotDataType = PinotDataType.getPinotDataTypeForExecution(columnDataType);
    _values = values;
  }



  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    // TODO: The assumption for now is that the rows in RowBasedBlockValSet contain non-null values.
    //  Update to pass nullBitmap in constructor if rows have null values. Alternatively, compute nullBitmap on the fly.
    return null;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getIntValuesSV() {
    int length = _values.size();
    int[] values = new int[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _values.get(i)).intValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Integer.parseInt((String) _values.get(i));
      }
    } else if (_dataType == FieldSpec.DataType.BOOLEAN) {
      for (int i = 0; i < length; i++) {
        values[i] = Boolean.compare((boolean) _values.get(i), false);
      }
    } else {
      throw new IllegalStateException("Cannot read int values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public long[] getLongValuesSV() {
    int length = _values.size();
    long[] values = new long[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _values.get(i)).longValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Long.parseLong((String) _values.get(i));
      }
    } else if (_dataType == FieldSpec.DataType.BOOLEAN) {
      for (int i = 0; i < length; i++) {
        values[i] = Boolean.compare((boolean) _values.get(i), false);
      }
    } else {
      throw new IllegalStateException("Cannot read long values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public float[] getFloatValuesSV() {
    int length = _values.size();
    float[] values = new float[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _values.get(i)).floatValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Float.parseFloat((String) _values.get(i));
      }
    } else if (_dataType == FieldSpec.DataType.BOOLEAN) {
      for (int i = 0; i < length; i++) {
        values[i] = Boolean.compare((boolean) _values.get(i), false);
      }
    } else {
      throw new IllegalStateException("Cannot read float values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public double[] getDoubleValuesSV() {
    int length = _values.size();
    double[] values = new double[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _values.get(i)).doubleValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Double.parseDouble((String) _values.get(i));
      }
    } else if (_dataType == FieldSpec.DataType.BOOLEAN) {
      for (int i = 0; i < length; i++) {
        values[i] = Boolean.compare((boolean) _values.get(i), false);
      }
    } else {
      throw new IllegalStateException("Cannot read double values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getStringValuesSV() {
    int length = _values.size();
    String[] values = new String[length];
    for (int i = 0; i < length; i++) {
      values[i] = _values.get(i).toString();
    }
    return values;
  }

  @Override
  public byte[][] getBytesValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getIntValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] getLongValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] getStringValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getNumMVEntries() {
    throw new UnsupportedOperationException();
  }
}

