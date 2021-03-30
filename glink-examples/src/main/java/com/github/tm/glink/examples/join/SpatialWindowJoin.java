package com.github.tm.glink.examples.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTReader;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * A simple example of how to use glink to perform spatial window join.
 *
 * @author Yu Liebing
 * */
public class SpatialWindowJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    GeometryFactory factory = new GeometryFactory();
    WKTReader wktReader = new WKTReader();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // point: id, wkt, time, name
    MapFunction<String, Geometry> mapFunction = text -> {
      String[] list = text.split(",");
      Geometry geometry = wktReader.read(list[1]);
      Date date = sdf.parse(list[2]);
      Tuple3<String, Long, String> t = new Tuple3<>(list[0], date.getTime(), list[3]);
      geometry.setUserData(t);
      return geometry;
    };

    SpatialDataStream<Geometry> pointSpatialDataStream1 =
            new SpatialDataStream<>(env, "localhost", 9000, mapFunction)
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Geometry>forBoundedOutOfOrderness(Duration.ZERO)
                            .withTimestampAssigner(
                                    (event, time) -> {
                                      System.out.println(event.getUserData());
                                      return ((Tuple3<String, Long, String>) event.getUserData()).f1;
                                    }));
    SpatialDataStream<Geometry> pointSpatialDataStream2 =
            new SpatialDataStream<>(env, "localhost", 9001, mapFunction)
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Geometry>forBoundedOutOfOrderness(Duration.ZERO)
                            .withTimestampAssigner(
                                    (event, time) -> ((Tuple3<String, Long, String>) event.getUserData()).f1));

    DataStream<Object> dataStream = pointSpatialDataStream1.spatialWindowJoin(
            pointSpatialDataStream2,
            TopologyType.WITHIN_DISTANCE,
            TumblingEventTimeWindows.of(Time.seconds(5)),
            (point, point2) -> point + ", " + point2,
            1);
    dataStream.print();
    pointSpatialDataStream1.print();

    env.execute();
  }
}
