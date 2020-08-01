package com.github.tm.glink.fearures.avro;

import com.github.tm.glink.fearures.Point;
import org.junit.Test;

import java.util.Date;

public class AvroPointTest {

  @Test
  public void serializeDeserializeTest() {
    Point point = new Point("123", 35.5, 114.13, new Date().getTime(), 123);
    AvroPoint avroPoint = new AvroPoint();
    long start = System.currentTimeMillis();
    // do 1000 serialize deserialize test
    for (int i = 0; i < 1000; ++i) {
      byte[] data = avroPoint.serialize(point);
      Point deserializePoint = avroPoint.deserialize(data);
    }
    long end = System.currentTimeMillis();

    System.out.println("1000 test time: " + (end - start));
  }

}