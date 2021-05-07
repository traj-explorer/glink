package com.github.tm.glink.examples.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SpatialFlatMapFunction extends RichFlatMapFunction<String, Geometry> {

  private WKTReader wktReader;
  private SimpleDateFormat sdf;

  @Override
  public void open(Configuration parameters) throws Exception {
    wktReader = new WKTReader();
    sdf = new SimpleDateFormat("HH:mm:ss");
  }

  @Override
  public void flatMap(String text, Collector<Geometry> collector) throws Exception {
    try {
      String[] list = text.split(",");
      Geometry geometry = wktReader.read(list[1]);
      Date date = sdf.parse(list[2]);
      Tuple2<String, Long> t = new Tuple2<>(list[0], date.getTime());
      geometry.setUserData(t);
      collector.collect(geometry);
    } catch (Exception e) {
      System.out.println("Cannot parse record: " + text);
    }
  }
}
