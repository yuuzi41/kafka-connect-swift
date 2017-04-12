package org.apache.kafka.connect.swift;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.TimeZone;

/**
 * Sample Bulker (CSV)
 */
public class KeyValueCsvSinkRecordBulker implements SinkRecordBulker {
  private ByteBuffer buffer;

  public KeyValueCsvSinkRecordBulker() {
    this.buffer = ByteBuffer.allocate(1024 * 1024);
  }

  public void put(SinkRecord record) {
    try {
      ByteArrayOutputStream resultStream = new ByteArrayOutputStream();
      Writer writer = new OutputStreamWriter(resultStream);

      Object key = record.key();
      if (key != null) {
        writer.write(key.toString());
        writer.write(',');
      }

      Object value = record.value();
      if (value != null) {
        writer.write(value.toString());
      }

      writer.write('\n');
      writer.close();

      this.buffer.put(resultStream.toByteArray());
    } catch (IOException exception) {
      //TODO: check exception
      throw new RuntimeException(exception);
    }
  }

  public Iterator<BulkedData> convert() {
    if (this.buffer.position() > 0) {
      byte[] binary = new byte[this.buffer.position()];
      System.arraycopy(this.buffer.array(), 0, binary, 0, this.buffer.position());
      this.buffer.clear();

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      String date = sdf.format(Calendar.getInstance().getTime());

      BulkedData bulkedData = new BulkedData("test", date, new ByteArrayInputStream(binary));

      return Collections.singleton(bulkedData).iterator();
    } else {
      return Collections.emptyIterator();
    }
  }
}
