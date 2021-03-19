package com.github.tm.glink.examples.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.SpatialJoinType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class SpatialDimensionJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    GeometryFactory factory = new GeometryFactory();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // point: id, lat, lng, time, name

    DataStream<Point> pointStream1 = env
            .socketTextStream("localhost", 9000)
            .map(text -> {
              String[] list = text.split(",");
              double lat = Double.parseDouble(list[1]);
              double lng = Double.parseDouble(list[2]);
              Point p = factory.createPoint(new Coordinate(lng, lat));
              Date date = sdf.parse(list[3]);
              Tuple3<String, Long, String> t = new Tuple3<>(list[0], date.getTime(), list[4]);
              p.setUserData(t);
              return p;
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((event, time) -> ((Tuple3<String, Long, String>) event.getUserData()).f1));
    SpatialDataStream<Point> pointSpatialDataStream1 = new SpatialDataStream<>(pointStream1);

    DataStream<Point> pointStream2 = env
            .socketTextStream("localhost", 9001)
            .map(text -> {
              String[] list = text.split(",");
              double lat = Double.parseDouble(list[1]);
              double lng = Double.parseDouble(list[2]);
              Point p = factory.createPoint(new Coordinate(lng, lat));
              Date date = sdf.parse(list[3]);
              Tuple3<String, Long, String> t = new Tuple3<>(list[0], date.getTime(), list[4]);
              p.setUserData(t);
              return p;
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((event, time) -> ((Tuple3<String, Long, String>) event.getUserData()).f1));
    SpatialDataStream<Point> pointSpatialDataStream2 = new SpatialDataStream<>(pointStream2);

    pointSpatialDataStream1.spatialDimensionJoin(
            pointSpatialDataStream2,
            SpatialJoinType.DISTANCE,
            (point, point2) -> point + ", " + point2,
            1);

    env.execute();
  }
}
