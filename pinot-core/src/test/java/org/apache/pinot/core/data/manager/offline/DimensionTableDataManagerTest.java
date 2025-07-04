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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderTest;
import org.apache.pinot.segment.local.utils.SegmentAllIndexPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentDownloadThrottler;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentMultiColTextIndexPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.SegmentStarTreePreprocessThrottler;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


@SuppressWarnings("unchecked")
public class DimensionTableDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), LoaderTest.class.getName());
  private static final String RAW_TABLE_NAME = "dimBaseballTeams";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String CSV_DATA_PATH = "data/dimBaseballTeams.csv";
  private static final String SCHEMA_PATH = "data/dimBaseballTeams_schema.json";
  private static final String TABLE_CONFIG_PATH = "data/dimBaseballTeams_config.json";
  private static final SegmentOperationsThrottler SEGMENT_OPERATIONS_THROTTLER = new SegmentOperationsThrottler(
      new SegmentAllIndexPreprocessThrottler(1, 2, true),
      new SegmentStarTreePreprocessThrottler(1, 2, true),
      new SegmentDownloadThrottler(1, 2, true),
      new SegmentMultiColTextIndexPreprocessThrottler(1, 2, true));

  private File _indexDir;
  private SegmentZKMetadata _segmentZKMetadata;

  @BeforeClass
  public void setUp()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));

    // prepare segment data
    URL dataPathUrl = getClass().getClassLoader().getResource(CSV_DATA_PATH);
    URL schemaPathUrl = getClass().getClassLoader().getResource(SCHEMA_PATH);
    URL configPathUrl = getClass().getClassLoader().getResource(TABLE_CONFIG_PATH);
    assertNotNull(dataPathUrl);
    assertNotNull(schemaPathUrl);
    assertNotNull(configPathUrl);
    File csvFile = new File(dataPathUrl.getFile());
    TableConfig tableConfig = createTableConfig(new File(configPathUrl.getFile()));
    Schema schema = createSchema(new File(schemaPathUrl.getFile()));

    // create segment
    File tableDataDir = new File(TEMP_DIR, OFFLINE_TABLE_NAME);

    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfig(csvFile, FileFormat.CSV, tableDataDir, RAW_TABLE_NAME, tableConfig,
            schema);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    String segmentName = driver.getSegmentName();
    _indexDir = new File(tableDataDir, segmentName);
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(_indexDir);
    _segmentZKMetadata = new SegmentZKMetadata(segmentName);
    _segmentZKMetadata.setCrc(Long.parseLong(segmentMetadata.getCrc()));
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private TableConfig getTableConfig(boolean disablePreload, boolean errorOnDuplicatePrimaryKey) {
    DimensionTableConfig dimensionTableConfig = new DimensionTableConfig(disablePreload, errorOnDuplicatePrimaryKey);
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("dimBaseballTeams")
        .setDimensionTableConfig(dimensionTableConfig)
        .build();
  }

  private Schema getSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName("dimBaseballTeams")
        .addSingleValueDimension("teamID", DataType.STRING)
        .addSingleValueDimension("teamName", DataType.STRING)
        .setPrimaryKeyColumns(Collections.singletonList("teamID"))
        .build();
  }

  private Schema getSchemaWithExtraColumn() {
    return new Schema.SchemaBuilder().setSchemaName("dimBaseballTeams")
        .addSingleValueDimension("teamID", DataType.STRING)
        .addSingleValueDimension("teamName", DataType.STRING)
        .addSingleValueDimension("teamCity", DataType.STRING)
        .setPrimaryKeyColumns(Collections.singletonList("teamID"))
        .build();
  }

  private DimensionTableDataManager makeTableDataManager(TableConfig tableConfig, Schema schema)
      throws IOException {
    return makeTableDataManager(tableConfig, schema, mock(ZkHelixPropertyStore.class));
  }

  private DimensionTableDataManager makeTableDataManager(TableConfig tableConfig, Schema schema,
      ZkHelixPropertyStore<ZNRecord> propertyStoreMock)
      throws JsonProcessingException {
    HelixManager helixManager = mock(HelixManager.class);
    when(propertyStoreMock.get("/CONFIGS/TABLE/dimBaseballTeams_OFFLINE", null, AccessOption.PERSISTENT)).thenReturn(
        TableConfigSerDeUtils.toZNRecord(tableConfig));
    when(propertyStoreMock.get("/SCHEMAS/dimBaseballTeams", null, AccessOption.PERSISTENT)).thenReturn(
        SchemaSerDeUtils.toZNRecord(schema));
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStoreMock);
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    DimensionTableDataManager tableDataManager =
        DimensionTableDataManager.createInstanceByTableName(OFFLINE_TABLE_NAME);
    tableDataManager.init(instanceDataManagerConfig, helixManager, new SegmentLocks(), tableConfig, schema,
        new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(), null, null, SEGMENT_OPERATIONS_THROTTLER);
    tableDataManager.start();
    return tableDataManager;
  }

  @Test
  public void testInstantiation()
      throws Exception {
    TableConfig tableConfig = getTableConfig(false, false);
    Schema schema = getSchema();
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema);
    assertEquals(tableDataManager.getTableName(), OFFLINE_TABLE_NAME);

    // fetch the same instance via static method
    DimensionTableDataManager returnedManager = DimensionTableDataManager.getInstanceByTableName(OFFLINE_TABLE_NAME);
    assertNotNull(returnedManager, "Manager should find instance");
    assertEquals(tableDataManager, returnedManager, "Manager should return already created instance");

    // assert that segments are released after loading data
    tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_OPERATIONS_THROTTLER));
    for (SegmentDataManager segmentManager : returnedManager.acquireAllSegments()) {
      assertEquals(segmentManager.getReferenceCount() - 1, // Subtract this acquisition
          1, // Default ref count
          "Reference counts should be same before and after segment loading.");
      returnedManager.releaseSegment(segmentManager);
      returnedManager.offloadSegment(segmentManager.getSegmentName());
    }

    // try fetching non-existent table
    returnedManager = DimensionTableDataManager.getInstanceByTableName("doesNotExist");
    assertNull(returnedManager, "Manager should return null for non-existent table");
  }

  @Test
  public void testLookup()
      throws Exception {
    TableConfig tableConfig = getTableConfig(false, false);
    Schema schema = getSchema();
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema);

    // try fetching data BEFORE loading segment
    PrimaryKey key = new PrimaryKey(new String[]{"SF"});
    assertFalse(tableDataManager.containsKey(key));
    assertNull(tableDataManager.lookupRow(key));
    assertNull(tableDataManager.lookupValue(key, "teamID"));
    assertNull(tableDataManager.lookupValue(key, "teamName"));
    assertNull(tableDataManager.lookupValues(key, new String[]{"teamID", "teamName"}));

    tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_OPERATIONS_THROTTLER));

    // Confirm table is loaded and available for lookup
    assertTrue(tableDataManager.containsKey(key));
    GenericRow row = tableDataManager.lookupRow(key);
    assertNotNull(row);
    assertEquals(row.getFieldToValueMap().size(), 2);
    assertEquals(row.getValue("teamID"), "SF");
    assertEquals(row.getValue("teamName"), "San Francisco Giants");
    assertEquals(tableDataManager.lookupValue(key, "teamID"), "SF");
    assertEquals(tableDataManager.lookupValue(key, "teamName"), "San Francisco Giants");
    Object[] values = tableDataManager.lookupValues(key, new String[]{"teamID", "teamName"});
    assertNotNull(values);
    assertEquals(values.length, 2);
    assertEquals(values[0], "SF");
    assertEquals(values[1], "San Francisco Giants");

    // Confirm we can get FieldSpec for loaded tables columns.
    FieldSpec spec = tableDataManager.getColumnFieldSpec("teamName");
    assertNotNull(spec, "Should return spec for existing column");
    assertEquals(spec.getDataType(), DataType.STRING, "Should return correct data type for teamName column");

    // Confirm we can read primary column list
    List<String> pkColumns = tableDataManager.getPrimaryKeyColumns();
    assertEquals(pkColumns, Collections.singletonList("teamID"), "Should return PK column list");

    // Remove the segment
    List<SegmentDataManager> segmentManagers = tableDataManager.acquireAllSegments();
    assertEquals(segmentManagers.size(), 1, "Should have exactly one segment manager");
    SegmentDataManager segMgr = segmentManagers.get(0);
    String segmentName = segMgr.getSegmentName();
    tableDataManager.offloadSegment(segmentName);
    // confirm table is cleaned up
    assertFalse(tableDataManager.containsKey(key));
    assertNull(tableDataManager.lookupRow(key));
    assertNull(tableDataManager.lookupValue(key, "teamID"));
    assertNull(tableDataManager.lookupValue(key, "teamName"));
    assertNull(tableDataManager.lookupValues(key, new String[]{"teamID", "teamName"}));
  }

  @Test
  public void testReloadTable()
      throws Exception {
    TableConfig tableConfig = getTableConfig(false, false);
    Schema schema = getSchema();
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema, propertyStore);
    tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_OPERATIONS_THROTTLER));

    // Confirm table is loaded and available for lookup
    PrimaryKey key = new PrimaryKey(new String[]{"SF"});
    assertTrue(tableDataManager.containsKey(key));
    GenericRow row = tableDataManager.lookupRow(key);
    assertNotNull(row);
    assertEquals(row.getFieldToValueMap().size(), 2);
    assertEquals(row.getValue("teamID"), "SF");
    assertEquals(row.getValue("teamName"), "San Francisco Giants");
    assertEquals(tableDataManager.lookupValue(key, "teamID"), "SF");
    assertEquals(tableDataManager.lookupValue(key, "teamName"), "San Francisco Giants");

    // Confirm the new column does not exist
    FieldSpec teamCitySpec = tableDataManager.getColumnFieldSpec("teamCity");
    assertNull(teamCitySpec, "Should not return spec for non-existing column");

    // Reload the segment with a new column
    Schema schemaWithExtraColumn = getSchemaWithExtraColumn();
    when(propertyStore.get("/SCHEMAS/dimBaseballTeams", null, AccessOption.PERSISTENT)).thenReturn(
        SchemaSerDeUtils.toZNRecord(schemaWithExtraColumn));
    when(propertyStore.get("/SEGMENTS/dimBaseballTeams_OFFLINE/" + _segmentZKMetadata.getSegmentName(), null,
        AccessOption.PERSISTENT)).thenReturn(_segmentZKMetadata.toZNRecord());
    tableDataManager.reloadSegment(_segmentZKMetadata.getSegmentName(), false);

    // Confirm the new column is available for lookup
    teamCitySpec = tableDataManager.getColumnFieldSpec("teamCity");
    assertNotNull(teamCitySpec, "Should return spec for existing column");
    assertEquals(teamCitySpec.getDataType(), DataType.STRING, "Should return correct data type for teamCity column");
    assertTrue(tableDataManager.containsKey(key));
    row = tableDataManager.lookupRow(key);
    assertNotNull(row, "Should return response after segment reload");
    assertEquals(row.getFieldToValueMap().size(), 3);
    assertEquals(row.getValue("teamID"), "SF");
    assertEquals(row.getValue("teamName"), "San Francisco Giants");
    assertEquals(row.getValue("teamCity"), "null");
    assertEquals(tableDataManager.lookupValue(key, "teamID"), "SF");
    assertEquals(tableDataManager.lookupValue(key, "teamName"), "San Francisco Giants");
    assertEquals(tableDataManager.lookupValue(key, "teamCity"), "null");
  }

  @Test
  public void testLookupWithoutPreLoad()
      throws Exception {
    TableConfig tableConfig = getTableConfig(true, false);
    Schema schema = getSchema();
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema);

    // try fetching data BEFORE loading segment
    PrimaryKey key = new PrimaryKey(new String[]{"SF"});
    assertFalse(tableDataManager.containsKey(key));
    assertNull(tableDataManager.lookupRow(key));

    tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_OPERATIONS_THROTTLER));

    // Confirm table is loaded and available for lookup
    assertTrue(tableDataManager.containsKey(key));
    GenericRow row = tableDataManager.lookupRow(key);
    assertNotNull(row, "Should return response after segment load");
    assertEquals(row.getFieldToValueMap().size(), 2);
    assertEquals(row.getValue("teamID"), "SF");
    assertEquals(row.getValue("teamName"), "San Francisco Giants");
    assertEquals(tableDataManager.lookupValue(key, "teamID"), "SF");
    assertEquals(tableDataManager.lookupValue(key, "teamName"), "San Francisco Giants");

    // Confirm we can get FieldSpec for loaded tables columns.
    FieldSpec spec = tableDataManager.getColumnFieldSpec("teamName");
    assertNotNull(spec, "Should return spec for existing column");
    assertEquals(spec.getDataType(), DataType.STRING, "Should return correct data type for teamName column");

    // Confirm we can read primary column list
    List<String> pkColumns = tableDataManager.getPrimaryKeyColumns();
    assertEquals(pkColumns, Collections.singletonList("teamID"), "Should return PK column list");

    // Remove the segment
    List<SegmentDataManager> segmentManagers = tableDataManager.acquireAllSegments();
    assertEquals(segmentManagers.size(), 1, "Should have exactly one segment manager");
    SegmentDataManager segMgr = segmentManagers.get(0);
    String segmentName = segMgr.getSegmentName();
    tableDataManager.offloadSegment(segmentName);
    // confirm table is cleaned up
    assertFalse(tableDataManager.containsKey(key));
    assertNull(tableDataManager.lookupRow(key));
  }

  @DataProvider(name = "options")
  private Object[] getOptions() {
    return new Boolean[]{true, false};
  }

  @Test(dataProvider = "options")
  public void testDeleteTableRemovesManagerFromMemory(boolean disablePreload)
      throws Exception {
    TableConfig tableConfig = getTableConfig(disablePreload, false);
    Schema schema = getSchema();
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema);

    tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_OPERATIONS_THROTTLER));

    tableDataManager.shutDown();

    Assert.assertNull(DimensionTableDataManager.getInstanceByTableName(tableDataManager.getTableName()));
  }

  @Test
  public void testLookupErrorOnDuplicatePrimaryKey()
      throws Exception {
    TableConfig tableConfig = getTableConfig(false, true);
    Schema schema = getSchema();
    DimensionTableDataManager tableDataManager = makeTableDataManager(tableConfig, schema);

    // try fetching data BEFORE loading segment
    assertFalse(tableDataManager.containsKey(new PrimaryKey(new String[]{"SF"})));

    try {
      tableDataManager.addSegment(ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
          SEGMENT_OPERATIONS_THROTTLER));
      fail("Should error out when ErrorOnDuplicatePrimaryKey is configured to true");
    } catch (Exception e) {
      // expected;
    }
  }

  protected static Schema createSchema(File schemaFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(schemaFile);
    Assert.assertNotNull(inputStream);
    return JsonUtils.inputStreamToObject(inputStream, Schema.class);
  }

  protected static TableConfig createTableConfig(File tableConfigFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(tableConfigFile);
    Assert.assertNotNull(inputStream);
    return JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
  }
}
