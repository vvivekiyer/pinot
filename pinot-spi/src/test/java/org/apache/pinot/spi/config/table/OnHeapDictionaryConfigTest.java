package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OnHeapDictionaryConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    OnHeapDictionaryConfig config = JsonUtils.stringToObject(confStr, OnHeapDictionaryConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    OnHeapDictionaryConfig config = JsonUtils.stringToObject(confStr, OnHeapDictionaryConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    OnHeapDictionaryConfig config = JsonUtils.stringToObject(confStr, OnHeapDictionaryConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    OnHeapDictionaryConfig config = JsonUtils.stringToObject(confStr, OnHeapDictionaryConfig.class);

    Assert.assertFalse(config.isEnabled(), "Config should be disabled");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "  \"enableInterning\": true,\n"
        + "  \"internerCapacity\": 1024\n"
        + "}";
    OnHeapDictionaryConfig config = JsonUtils.stringToObject(confStr, OnHeapDictionaryConfig.class);

    Assert.assertTrue(config.isEnabled(), "Config should be enabled");
    Assert.assertEquals(config.enableInterning(), true, "Interning should be enabled");
    Assert.assertEquals(config.internerCapacity(), 1024, "internerCapacity is wrong");
  }

}
