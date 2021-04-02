package com.github.tm.glink.examples.common;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SpatialMapFunction extends RichMapFunction<String, Geometry> {

  private WKTReader wktReader;
  private SimpleDateFormat sdf;

  @Override
  public void open(Configuration parameters) throws Exception {
    wktReader = new WKTReader();
    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  @Override
  public Geometry map(String text) throws Exception {
    String[] list = text.split(",");
    Geometry geometry = wktReader.read(list[1]);
    Date date = sdf.parse(list[2]);
    Tuple2<String, Long> t = new Tuple2<>(list[0], date.getTime());
    geometry.setUserData(t);
    return geometry;
  }
}
