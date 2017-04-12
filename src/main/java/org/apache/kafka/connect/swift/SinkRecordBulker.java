package org.apache.kafka.connect.swift;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Iterator;

public interface SinkRecordBulker {
  void put(SinkRecord record);
  Iterator<BulkedData> convert();
}
