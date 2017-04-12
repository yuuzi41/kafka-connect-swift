package org.apache.kafka.connect.swift;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka Sink Connector for OpenStack Swift.
 */
public class SwiftSinkConnector extends SinkConnector {
  /** Sink properties. */
  private Map<String, String> configProps;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = props;

    try {
      new SwiftSinkConnectorConfig(props);
    } catch (ConfigException exception) {
      throw new ConnectException("Cannot start SwiftSinkConnector due to configuration error", exception);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SwiftSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();

    taskProps.putAll(configProps);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return SwiftSinkConnectorConfig.config;
  }
}
