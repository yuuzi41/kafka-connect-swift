package org.apache.kafka.connect.swift;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class KeyValueCsvSinkRecordBulkerTest {
  @Test
  public void testConvert() throws Exception {
    KeyValueCsvSinkRecordBulker conv = new KeyValueCsvSinkRecordBulker();

    SinkRecord testdata1 = mock(SinkRecord.class);
    when(testdata1.key()).thenReturn("abc");
    when(testdata1.value()).thenReturn("def");
    conv.put(testdata1);

    SinkRecord testdata2 = mock(SinkRecord.class);
    when(testdata2.key()).thenReturn("ghi");
    when(testdata2.value()).thenReturn("jkl");
    conv.put(testdata2);

    Iterator<BulkedData> iterator = conv.convert();

    while(iterator.hasNext()) {
      BulkedData data = iterator.next();
      BufferedReader reader = new BufferedReader(new InputStreamReader(data.getStream()));

      final String line1 = reader.readLine();
      {
        String[] kvstr = line1.split(",");
        Assert.assertEquals(2, kvstr.length);

        Assert.assertEquals("abc", kvstr[0]);
        Assert.assertEquals("def",kvstr[1]);
      }

      final String line2 = reader.readLine();
      {
        String[] kvstr = line2.split(",");
        Assert.assertEquals(2, kvstr.length);

        Assert.assertEquals("ghi", kvstr[0]);
        Assert.assertEquals("jkl",kvstr[1]);
      }
    }
  }
}
