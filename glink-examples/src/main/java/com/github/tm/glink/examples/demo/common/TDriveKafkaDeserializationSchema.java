package com.github.tm.glink.examples.demo.common;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author Yu Liebing
 * */
public class TDriveKafkaDeserializationSchema implements DeserializationSchema<Point> {

  private DateTimeFormatter dateTimeFormatter;
  private GeometryFactory factory;

  @Override
  public void open(InitializationContext context) throws Exception {
    dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    factory = new GeometryFactory();
  }

  @Override
  public Point deserialize(byte[] bytes) throws IOException {
    try {
      String text = new String(bytes);
      String[] list = text.split(",");
      int id = Integer.parseInt(list[0]);
      LocalDateTime dateTime = LocalDateTime.parse(list[1], dateTimeFormatter);
      double lng = Double.parseDouble(list[2]);
      double lat = Double.parseDouble(list[3]);
      if (lng < -180 || lng > 180) return null;
      if (lat < -90 || lat > 90) return null;
      Point point = factory.createPoint(new Coordinate(lng, lat));
      Tuple2<Integer, Long> attr = new Tuple2<>(id, dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());
      point.setUserData(attr);
      return point;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public boolean isEndOfStream(Point point) {
    return false;
  }

  @Override
  public TypeInformation<Point> getProducedType() {
    return TypeInformation.of(Point.class);
  }
}
