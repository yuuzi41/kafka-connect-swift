package org.apache.kafka.connect.swift;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SwiftSinkConnectorTest {
  private SwiftSinkConnector connector = new SwiftSinkConnector();

  @Test
  public void testVersion() throws Exception {
    Assert.assertTrue(connector.version().length() > 0);
  }

  @Test
  public void testStart() throws Exception {
    connector.start(
        ImmutableMap.of(
            SwiftSinkConnectorConfig.SWIFT_ENDPOINT, "http://test.org",
            SwiftSinkConnectorConfig.SWIFT_AUTHTYPE, "tempauth",
            SwiftSinkConnectorConfig.SWIFT_USERNAME, "tester",
            SwiftSinkConnectorConfig.SWIFT_PASSWORD, "testing",
            SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG, KeyValueCsvSinkRecordBulker.class.getCanonicalName()
        )
    );
  }

  @Test(expected = ConnectException.class)
  public void testStartFail() throws Exception {
    connector.start(
        ImmutableMap.of(
            SwiftSinkConnectorConfig.SWIFT_ENDPOINT, "http://test.org",
            SwiftSinkConnectorConfig.SWIFT_AUTHTYPE, "tempauth",
            SwiftSinkConnectorConfig.SWIFT_USERNAME, "tester",
            SwiftSinkConnectorConfig.SWIFT_PASSWORD, "testing",
            SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG,"fuga"
        )
    );
  }

  @Test
  public void testTaskClass() throws Exception {
    Assert.assertEquals(SwiftSinkTask.class, connector.taskClass());
  }

  @Test
  public void testTaskConfigs() throws Exception {
    connector.start(
        ImmutableMap.of(
            SwiftSinkConnectorConfig.SWIFT_ENDPOINT, "http://test.org",
            SwiftSinkConnectorConfig.SWIFT_AUTHTYPE, "tempauth",
            SwiftSinkConnectorConfig.SWIFT_USERNAME, "tester",
            SwiftSinkConnectorConfig.SWIFT_PASSWORD, "testing",
            SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG, KeyValueCsvSinkRecordBulker.class.getCanonicalName()
        )
    );
    List<Map<String, String>> configs = connector.taskConfigs(5);

    Assert.assertEquals(5, configs.size());
    for (Map<String,String> conf : configs) {
      Assert.assertEquals(5, conf.size());

      Assert.assertTrue(conf.containsKey(SwiftSinkConnectorConfig.SWIFT_ENDPOINT));
      Assert.assertEquals("http://test.org",conf.get(SwiftSinkConnectorConfig.SWIFT_ENDPOINT));

      Assert.assertTrue(conf.containsKey(SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG));
      Assert.assertEquals(
          KeyValueCsvSinkRecordBulker.class.getCanonicalName(),
          conf.get(SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG)
      );
    }
  }

  @Test
  public void testStop() throws Exception {
    connector.start(
        ImmutableMap.of(
            SwiftSinkConnectorConfig.SWIFT_ENDPOINT, "http://test.org",
            SwiftSinkConnectorConfig.SWIFT_AUTHTYPE, "tempauth",
            SwiftSinkConnectorConfig.SWIFT_USERNAME, "tester",
            SwiftSinkConnectorConfig.SWIFT_PASSWORD, "testing",
            SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG, KeyValueCsvSinkRecordBulker.class.getCanonicalName()
        )
    );

    connector.stop();
  }

  @Test
  public void testConfig() throws Exception {
    ConfigDef configDef = connector.config();

    Assert.assertEquals(SwiftSinkConnectorConfig.config, configDef);
  }

}