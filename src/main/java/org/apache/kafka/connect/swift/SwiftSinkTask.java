package org.apache.kafka.connect.swift;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Kafka Sink Connector Task for OpenStack Swift.
 */
public class SwiftSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(SwiftSinkTask.class);

  private SwiftSinkConnectorConfig config;

  private SinkRecordBulker bulker;
  private Account account;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    try {
      config = new SwiftSinkConnectorConfig(props);
    } catch (ConfigException exception) {
      throw new ConnectException("Cannot start SwiftSinkConnector due to configuration error", exception);
    }

    AccountConfig accountConfig = new AccountConfig();
    accountConfig.setAuthUrl(config.getString(SwiftSinkConnectorConfig.SWIFT_ENDPOINT));
    accountConfig.setUsername(config.getString(SwiftSinkConnectorConfig.SWIFT_USERNAME));
    accountConfig.setPassword(config.getString(SwiftSinkConnectorConfig.SWIFT_PASSWORD));

    switch (config.getString(SwiftSinkConnectorConfig.SWIFT_AUTHTYPE)) {
      case "keystone":
        accountConfig.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
        break;
      case "tempauth":
        accountConfig.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
        break;
      case "basic":
        accountConfig.setAuthenticationMethod(AuthenticationMethod.BASIC);
        break;
      default:
        throw new ConnectException("Cannot recognize authtype.");
    }

    if (!config.getString(SwiftSinkConnectorConfig.SWIFT_TENANT_NAME).isEmpty()) {
      accountConfig.setTenantName(config.getString(SwiftSinkConnectorConfig.SWIFT_TENANT_NAME));
    }
    if (!config.getString(SwiftSinkConnectorConfig.SWIFT_TENANT_ID).isEmpty()) {
      accountConfig.setTenantId(config.getString(SwiftSinkConnectorConfig.SWIFT_TENANT_ID));
    }
    this.account = new AccountFactory(accountConfig).createAccount();

    try {
      Class<? extends SinkRecordBulker> bulkerClass =
          (Class<? extends SinkRecordBulker>) config.getClass(SwiftSinkConnectorConfig.SWIFT_CONVERTER_CLASS_CONFIG);
      bulker = bulkerClass.newInstance();
    } catch (Exception exception) {
      throw new ConnectException("Failed to Instantiation.", exception);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    try {
      for (SinkRecord record : records) {
        bulker.put(record);
      }
    } catch (Exception exception) {
      log.error("Failed to add record", exception);

      throw new ConnectException(exception);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    try {
      Iterator<BulkedData> items = bulker.convert();

      while (items.hasNext()) {
        final BulkedData item = items.next();

        final Container container = this.account.getContainer(item.getContainer());
        if (!container.exists()) {
          container.create();
        }

        final StoredObject object = container.getObject(item.getObject());
        object.uploadObject(item.getStream());
      }
    } catch (Exception exception) {
      log.error("Failed to add record", exception);

      throw new ConnectException("Failed to flush", exception);
    }
  }

  @Override
  public void stop() {

  }
}
