package com.github.tm.glink.examples.common;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BroadcastFlatMapFunction extends RichFlatMapFunction<String, Tuple2<Boolean, Geometry>> {

  private WKTReader wktReader;
  private SimpleDateFormat sdf;

  @Override
  public void open(Configuration parameters) throws Exception {
    wktReader = new WKTReader();
    sdf = new SimpleDateFormat("HH:mm:ss");
  }

  @Override
  public void flatMap(String text, Collector<Tuple2<Boolean, Geometry>> collector) throws Exception {
    try {
      String[] list = text.split(",");
      Geometry geometry = wktReader.read(list[1]);
      Date date = sdf.parse(list[2]);
      Tuple2<String, Long> t = new Tuple2<>(list[0], date.getTime());
      geometry.setUserData(t);
      collector.collect(new Tuple2<>(true, geometry));
    } catch (Exception e) {
      System.out.println("Cannot parse record: " + text);
    }
  }
}
