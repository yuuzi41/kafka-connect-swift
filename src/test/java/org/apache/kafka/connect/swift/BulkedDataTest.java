package org.apache.kafka.connect.swift;

import org.junit.Assert;
import org.junit.Test;

public class BulkedDataTest {
  @Test
  public void testGetter() throws Exception {
    BulkedData data = new BulkedData("testCon","testObj",null);

    Assert.assertEquals("testCon", data.getContainer());
    Assert.assertEquals("testObj", data.getObject());
    Assert.assertEquals(null, data.getStream());
  }

}