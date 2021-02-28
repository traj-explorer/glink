package com.github.tm.glink.examples.window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class WindowExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(1000L);

    GeometryFactory factory = new GeometryFactory();
    DataStream<Point> dataStream = env.fromElements(factory.createPoint(new Coordinate(1, 2)),
            factory.createPoint(new Coordinate(3, 4)))
            .map(r -> r)
            .assignTimestampsAndWatermarks(new TimeAssigner());

    dataStream.keyBy(r -> r).timeWindow(Time.seconds(1)).apply(new WindowFunction<Point, Point, Point, TimeWindow>() {
      @Override
      public void apply(Point point, TimeWindow timeWindow, Iterable<Point> iterable, Collector<Point> collector) throws Exception {
        collector.collect(point);
      }
    }).print();

    env.execute();
  }
}
