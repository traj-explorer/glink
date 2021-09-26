package com.github.tm.glink.examples.analysis;

import com.github.tm.glink.core.analysis.CGG;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.index.GeographicalGridIndex;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class CGGExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SpatialDataStream.gridIndex = new GeographicalGridIndex(
            99.99948077714359, 100.00207689142543, 29.998201359264474, 30.00044966018388,
            2, 2);
    SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
            env,
            "/home/liebing/Code/glink/glink-examples/src/main/resources/cgg_test.txt",
            new SimpleSTPointFlatMapper())
            .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);
    CGG.generate(pointDataStream, 0.0505);

    env.execute();
  }

  public static class SimpleSTPointFlatMapper extends RichFlatMapFunction<String, Point> {

    private GeometryFactory factory;
    private DateTimeFormatter formatter;

    @Override
    public void open(Configuration parameters) throws Exception {
      factory = new GeometryFactory();
      formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void flatMap(String line, Collector<Point> collector) throws Exception {
      try {
        String[] items = line.split(",");
        int id = Integer.parseInt(items[0]);
        double lng = Double.parseDouble(items[1]);
        double lat = Double.parseDouble(items[2]);
        long timestamp = LocalDateTime.parse(items[3], formatter).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        int flag = Integer.parseInt(items[4]);
        Point p = factory.createPoint(new Coordinate(lng, lat));
        p.setUserData(new Tuple3<>(id, timestamp, flag));
        collector.collect(p);
      } catch (Exception e) {
        System.out.println("Cannot parse: " + line);
      }
    }
  }
}
