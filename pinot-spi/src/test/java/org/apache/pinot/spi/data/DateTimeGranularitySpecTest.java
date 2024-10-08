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

package org.apache.pinot.spi.data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class DateTimeGranularitySpecTest {

  // Test construct granularity from components
  @Test(dataProvider = "testConstructGranularityDataProvider")
  public void testConstructGranularity(int size, TimeUnit unit, DateTimeGranularitySpec granularityExpected) {
    DateTimeGranularitySpec granularityActual = null;
    try {
      granularityActual = new DateTimeGranularitySpec(size, unit);
    } catch (Exception e) {
      // invalid arguments
    }
    assertEquals(granularityActual, granularityExpected);
  }

  @DataProvider(name = "testConstructGranularityDataProvider")
  public Object[][] provideTestConstructGranularityData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[]{1, TimeUnit.HOURS, new DateTimeGranularitySpec("1:HOURS")});
    entries.add(new Object[]{5, TimeUnit.MINUTES, new DateTimeGranularitySpec("5:MINUTES")});
    entries.add(new Object[]{0, TimeUnit.HOURS, null});
    entries.add(new Object[]{-1, TimeUnit.HOURS, null});
    entries.add(new Object[]{1, null, null});

    return entries.toArray(new Object[entries.size()][]);
  }

  // Test granularity to millis
  @Test(dataProvider = "testGranularityToMillisDataProvider")
  public void testGranularityToMillis(String granularity, Long millisExpected) {
    Long millisActual = null;
    DateTimeGranularitySpec granularitySpec = null;
    try {
      granularitySpec = new DateTimeGranularitySpec(granularity);
      millisActual = granularitySpec.granularityToMillis();
    } catch (Exception e) {
      // invalid arguments
    }
    assertEquals(millisActual, millisExpected);
  }

  @DataProvider(name = "testGranularityToMillisDataProvider")
  public Object[][] provideTestGranularityToMillisData() {

    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[]{"1:HOURS", 3600000L});
    entries.add(new Object[]{"1:MILLISECONDS", 1L});
    entries.add(new Object[]{"15:MINUTES", 900000L});
    entries.add(new Object[]{"0:HOURS", null});
    entries.add(new Object[]{null, null});
    entries.add(new Object[]{"1:DUMMY", null});

    return entries.toArray(new Object[entries.size()][]);
  }

  @Test
  public void testDateTimeGranularitySpec() {
    // Old format
    DateTimeGranularitySpec dateTimeGranularitySpec = new DateTimeGranularitySpec("1:HOURS");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.HOURS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 3600000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("15:MINUTES");
    assertEquals(dateTimeGranularitySpec.getSize(), 15);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MINUTES);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 900000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("1:MILLISECONDS");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 1);

    // New format
    dateTimeGranularitySpec = new DateTimeGranularitySpec("HOURS|1");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.HOURS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 3600000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("MINUTES|15");
    assertEquals(dateTimeGranularitySpec.getSize(), 15);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MINUTES);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 900000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("MILLISECONDS|1");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 1);

    // New format without explicitly setting size
    dateTimeGranularitySpec = new DateTimeGranularitySpec("HOURS");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.HOURS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 3600000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("MINUTES");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MINUTES);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 60000);

    dateTimeGranularitySpec = new DateTimeGranularitySpec("MILLISECONDS");
    assertEquals(dateTimeGranularitySpec.getSize(), 1);
    assertEquals(dateTimeGranularitySpec.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(dateTimeGranularitySpec.granularityToMillis(), 1);

    // IllegalArguments
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec(""));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1:1"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1|1"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("DAY:DAY"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("DAY|DAY"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("DAY:1"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1|DAY"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("EPOCH|DAY|1"));
    assertThrows(IllegalArgumentException.class, () -> new DateTimeGranularitySpec("1:DAY:EPOCH"));
  }
}
