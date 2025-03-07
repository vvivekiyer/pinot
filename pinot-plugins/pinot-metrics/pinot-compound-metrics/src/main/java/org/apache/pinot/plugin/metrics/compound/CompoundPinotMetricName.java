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
package org.apache.pinot.plugin.metrics.compound;

import java.util.List;
import java.util.Objects;
import org.apache.pinot.spi.metrics.PinotMetricName;


public class CompoundPinotMetricName implements PinotMetricName {
  private final String _toString;
  private final List<PinotMetricName> _names;

  public CompoundPinotMetricName(String toString, List<PinotMetricName> names) {
    _toString = toString;
    _names = names;
  }

  @Override
  public String toString() {
    return _toString;
  }

  @Override
  public List<PinotMetricName> getMetricName() {
    return _names;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompoundPinotMetricName that = (CompoundPinotMetricName) o;
    return Objects.equals(_names, that._names);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_names);
  }
}
