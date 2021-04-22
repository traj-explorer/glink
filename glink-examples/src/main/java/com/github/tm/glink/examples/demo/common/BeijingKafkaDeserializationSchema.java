package com.github.tm.glink.examples.demo.common;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;

/**
 * @author Yu Liebing
 * */
public class BeijingKafkaDeserializationSchema implements DeserializationSchema<Tuple2<Boolean, Geometry>> {

  private WKTReader wktReader;

  @Override
  public void open(InitializationContext context) throws Exception {
    wktReader = new WKTReader();
  }

  @Override
  public Tuple2<Boolean, Geometry> deserialize(byte[] bytes) throws IOException {
    try {
      String text = new String(bytes);
      String[] list = text.split(";");
      int id = Integer.parseInt(list[0]);
      String name = list[1];
      Geometry geom = wktReader.read(list[2]);
      Tuple2<Integer, String> attr = new Tuple2<>(id, name);
      geom.setUserData(attr);
      return new Tuple2<>(true, geom);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public boolean isEndOfStream(Tuple2<Boolean, Geometry> tuple2) {
    return false;
  }

  @Override
  public TypeInformation<Tuple2<Boolean, Geometry>> getProducedType() {
    return TypeInformation.of(new TypeHint<Tuple2<Boolean, Geometry>>() { });
  }
}
