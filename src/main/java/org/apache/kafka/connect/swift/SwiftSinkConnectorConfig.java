package org.apache.kafka.connect.swift;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Kafka Sink Connector Config for OpenStack Swift.
 */
public class SwiftSinkConnectorConfig extends AbstractConfig {

  private static final Logger log = LoggerFactory.getLogger(SwiftSinkConnectorConfig.class);

  public static final String SWIFT_ENDPOINT = "swift.endpoint";
  private static final String SWIFT_ENDPOINT_DOC = "URL for Swift";

  public static final String SWIFT_USERNAME = "swift.auth.username";
  private static final String SWIFT_USERNAME_DOC = "Username for authentication.";

  public static final String SWIFT_PASSWORD = "swift.auth.password";
  private static final String SWIFT_PASSWORD_DOC = "Password for authentication.";

  public static final String SWIFT_AUTHTYPE = "swift.auth.type";
  private static final String SWIFT_AUTHTYPE_DOC = "Authentication Type. keystone or tempauth or basic";

  public static final String SWIFT_TENANT_ID = "swift.tenant_id";
  private static final String SWIFT_TENANT_ID_DOC = "Tenant id";

  public static final String SWIFT_TENANT_NAME = "swift.tenant_name";
  private static final String SWIFT_TENANT_NAME_DOC = "Tenant name";

  public static final String SWIFT_CONVERTER_CLASS_CONFIG = "swift.converter";
  private static final String SWIFT_CONVERTER_CLASS_DOC = "Converter class that converts Connect data to Binary format.";

  static ConfigDef config = new ConfigDef()
      .define(SWIFT_ENDPOINT, ConfigDef.Type.STRING, "http://localhost:8080", ConfigDef.Importance.HIGH, SWIFT_ENDPOINT_DOC)
      .define(SWIFT_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SWIFT_USERNAME_DOC)
      .define(SWIFT_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SWIFT_PASSWORD_DOC)
      .define(SWIFT_AUTHTYPE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SWIFT_AUTHTYPE_DOC)
      .define(SWIFT_TENANT_ID, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, SWIFT_TENANT_ID_DOC)
      .define(SWIFT_TENANT_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, SWIFT_TENANT_NAME_DOC)
      .define(SWIFT_CONVERTER_CLASS_CONFIG, ConfigDef.Type.CLASS, ConfigDef.Importance.MEDIUM, SWIFT_CONVERTER_CLASS_DOC);


  public SwiftSinkConnectorConfig(Map<String, String> props) {
    super(config, props);
  }

}
