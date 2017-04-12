package org.apache.kafka.connect.swift;

import java.io.InputStream;

/**
 * POJO expression of a Object that putting to OpenStack Swift.
 */
public class BulkedData {
  /** name of container. */
  private String container;

  /** name of object. */
  private String object;

  /** stream of object putting. */
  private InputStream stream;

  public BulkedData(String container, String object, InputStream stream) {
    this.container = container;
    this.object = object;
    this.stream = stream;
  }

  public String getContainer() {
    return container;
  }

  public String getObject() {
    return object;
  }

  public InputStream getStream() {
    return stream;
  }
}
