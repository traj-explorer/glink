package com.github.tm.glink.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import com.github.tm.glink.feature.Point;

import java.io.IOException;

public class PointSchema implements  DeserializationSchema<Point>, SerializationSchema<Point> {

  @Override
  public Point deserialize(byte[] bytes) throws IOException {
    return Point.fromString(new String(bytes));
  }

  @Override
  public boolean isEndOfStream(Point point) {
    return false;
  }

  @Override
  public byte[] serialize(Point point) {
    return point.toString().getBytes();
  }

  @Override
  public TypeInformation<Point> getProducedType() {
    return TypeExtractor.getForClass(Point.class);
  }
}
