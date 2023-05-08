package org.apache.pinot.common.request.context;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.data.FieldSpec;


public class IdentifierContext {
  // TODO(Vivek): Check if we can use FieldSpec.DataType
  private DataSchema.ColumnDataType _dataType;
  String _name;

  // Identifier Index is needed because V2 Engine identifies identifier with the index position.
  int _identifierIndex;



  public IdentifierContext(String name, DataSchema.ColumnDataType dataType, int identifierIndex) {
    _name = name;
    _dataType = dataType;
    _identifierIndex = identifierIndex;
  }

  public String getName() {
    return _name;
  }

  public DataSchema.ColumnDataType getDataType() {
    return _dataType;
  }

  public int getIdentifierIndex() {
    return _identifierIndex;
  }
}
