package org.apache.kafka.connect.swift;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SwiftSinkConnectorConfigTest {
  @Test
  public void testParse() {
    Map<String,String> testProps = ImmutableMap.of(
        SwiftSinkConnectorConfig.SWIFT_ENDPOINT, "http://test.org",
        SwiftSinkConnectorConfig.SWIFT_AUTHTYPE, "tempauth",
        SwiftSinkConnectorConfig.SWIFT_USERNAME, "tester",
        SwiftSinkConnectorConfig.SWIFT_PASSWORD, "testing",
        SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG, KeyValueCsvSinkRecordBulker.class.getCanonicalName()

    );
    SwiftSinkConnectorConfig config = new SwiftSinkConnectorConfig(testProps);

    Assert.assertEquals("http://test.org",
        config.getString(SwiftSinkConnectorConfig.SWIFT_ENDPOINT)
    );
    Assert.assertEquals(KeyValueCsvSinkRecordBulker.class,
        config.getClass(SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG)
    );
  }

  @Test(expected = ConfigException.class)
  public void testParseFail() {
    Map<String,String> testProps = ImmutableMap.of(
        SwiftSinkConnectorConfig.SWIFT_ENDPOINT, "http://test.org",
        SwiftSinkConnectorConfig.SWIFT_AUTHTYPE, "tempauth",
        SwiftSinkConnectorConfig.SWIFT_USERNAME, "tester",
        SwiftSinkConnectorConfig.SWIFT_PASSWORD, "testing",
        SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG,"fuga"
    );
    SwiftSinkConnectorConfig config = new SwiftSinkConnectorConfig(testProps);
  }
}