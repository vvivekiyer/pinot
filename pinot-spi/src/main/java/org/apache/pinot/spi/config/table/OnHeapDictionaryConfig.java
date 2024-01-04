package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;


public class OnHeapDictionaryConfig extends IndexConfig {
  private final boolean _enableInterning;
  private final int _internerCapacity;

  public OnHeapDictionaryConfig(boolean enableInterning, int internerCapacity) {
    this(false, enableInterning, internerCapacity);
  }

  @JsonCreator
  public OnHeapDictionaryConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty(value = "enableInterning") boolean enableInterning,
      @JsonProperty(value = "internerCapacity") int internerCapacity) {
    super(disabled);
    _enableInterning = enableInterning;
    _internerCapacity = internerCapacity;
  }

  public boolean enableInterning() {
    return _enableInterning;
  }

  public int internerCapacity() { return _internerCapacity; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    OnHeapDictionaryConfig that = (OnHeapDictionaryConfig) o;
    return _enableInterning == that._enableInterning && isEnabled() == that.isEnabled() && _internerCapacity == that._internerCapacity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _enableInterning, _internerCapacity, isEnabled());
  }
}
