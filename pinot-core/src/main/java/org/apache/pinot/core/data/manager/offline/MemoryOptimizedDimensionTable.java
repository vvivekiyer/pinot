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
package org.apache.pinot.core.data.manager.offline;

import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MemoryOptimizedDimensionTable implements DimensionTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryOptimizedDimensionTable.class);

  private final Object2LongOpenCustomHashMap<Object[]> _lookupTable;
  private final Schema _tableSchema;
  private final List<String> _primaryKeyColumns;
  private final ThreadLocal<GenericRow> _reuse = ThreadLocal.withInitial(GenericRow::new);
  private final List<SegmentDataManager> _segmentDataManagers;
  private final List<PinotSegmentRecordReader> _recordReaders;
  private final TableDataManager _tableDataManager;

  MemoryOptimizedDimensionTable(Schema tableSchema,
      List<String> primaryKeyColumns,
      Object2LongOpenCustomHashMap<Object[]> lookupTable,
      List<SegmentDataManager> segmentDataManagers,
      List<PinotSegmentRecordReader> recordReaders,
      TableDataManager tableDataManager) {
    _tableSchema = tableSchema;
    _primaryKeyColumns = primaryKeyColumns;
    _lookupTable = lookupTable;
    _segmentDataManagers = segmentDataManagers;
    _recordReaders = recordReaders;
    _tableDataManager = tableDataManager;
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Override
  public FieldSpec getFieldSpecFor(String columnName) {
    return _tableSchema.getFieldSpecFor(columnName);
  }

  @Override
  public boolean isEmpty() {
    return _lookupTable.isEmpty();
  }

  @Override
  public boolean containsKey(PrimaryKey pk) {
    return _lookupTable.containsKey(pk.getValues());
  }

  @Nullable
  @Override
  public GenericRow getRow(PrimaryKey pk) {
    long readerIdxAndDocId = _lookupTable.getLong(pk.getValues());
    if (readerIdxAndDocId < 0) {
      return null;
    }

    GenericRow reuse = _reuse.get();
    reuse.clear();

    int docId = (int) (readerIdxAndDocId & 0xffffffffL);
    int readerIdx = (int) (readerIdxAndDocId >> 32);

    PinotSegmentRecordReader recordReader = _recordReaders.get(readerIdx);
    recordReader.getRecord(docId, reuse);
    return reuse;
  }

  @Nullable
  @Override
  public Object getValue(PrimaryKey pk, String columnName) {
    long readerIdxAndDocId = _lookupTable.getLong(pk.getValues());
    if (readerIdxAndDocId < 0) {
      return null;
    }

    int docId = (int) (readerIdxAndDocId & 0xffffffffL);
    int readerIdx = (int) (readerIdxAndDocId >> 32);

    PinotSegmentRecordReader recordReader = _recordReaders.get(readerIdx);
    return recordReader.getValue(docId, columnName);
  }

  @Nullable
  @Override
  public Object[] getValues(PrimaryKey pk, String[] columnNames) {
    long readerIdxAndDocId = _lookupTable.getLong(pk.getValues());
    if (readerIdxAndDocId < 0) {
      return null;
    }

    int docId = (int) (readerIdxAndDocId & 0xffffffffL);
    int readerIdx = (int) (readerIdxAndDocId >> 32);
    PinotSegmentRecordReader recordReader = _recordReaders.get(readerIdx);

    int numColumns = columnNames.length;
    Object[] values = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      values[i] = recordReader.getValue(docId, columnNames[i]);
    }
    return values;
  }

  @Override
  public void close() {
    for (PinotSegmentRecordReader recordReader : _recordReaders) {
      try {
        recordReader.close();
      } catch (Exception e) {
        LOGGER.error("Caught exception while closing record reader for segment: {}", recordReader.getSegmentName(), e);
      }
    }
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      _tableDataManager.releaseSegment(segmentDataManager);
    }
  }
}
