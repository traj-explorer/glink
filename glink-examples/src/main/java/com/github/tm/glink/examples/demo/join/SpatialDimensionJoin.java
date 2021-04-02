package com.github.tm.glink.examples.demo.join;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SpatialDimensionJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String trajectoryPath = parameterTool.get("tPath");
    String distinctPath = parameterTool.get("dPath");

    SpatialDataStream<Point> spatialDataStream1 = new SpatialDataStream<>(env, trajectoryPath, new TrajectoryFlatMapFunction());
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 = new BroadcastSpatialDataStream<>(env, distinctPath, new DistinctFlatMapFunction());

    spatialDataStream1.spatialDimensionJoin(
            spatialDataStream2,
            TopologyType.N_CONTAINS,
            (point, geometry) -> {
              Tuple2<Integer, Long> pointAttr = (Tuple2<Integer, Long>) point.getUserData();
              Tuple2<Integer, String> distinctAttr = (Tuple2<Integer, String>) geometry.getUserData();
              return pointAttr.f0 + "," + distinctAttr.f0 + "," + distinctAttr.f1;
            },
            new TypeHint<String>() {
            })
            .print();

    env.execute();
  }

  public static class TrajectoryFlatMapFunction extends RichFlatMapFunction<String, Point> {

    private GeometryFactory factory;
    private SimpleDateFormat sdf;

    @Override
    public void open(Configuration parameters) throws Exception {
      factory = new GeometryFactory();
      sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void flatMap(String text, Collector<Point> collector) throws Exception {
      try {
        String[] list = text.split(",");
        int id = Integer.parseInt(list[0]);
        Date date = sdf.parse(list[1]);
        double lng = Double.parseDouble(list[2]);
        double lat = Double.parseDouble(list[3]);
        Point p = factory.createPoint(new Coordinate(lng, lat));
        p.setUserData(new Tuple2<>(id, date.getTime()));
        collector.collect(p);
      } catch (Exception e) {
        System.out.println("Cannot parse recode: " + text);
      }
    }
  }

  public static class DistinctFlatMapFunction extends RichFlatMapFunction<String, Tuple2<Boolean, Geometry>> {

    private WKTReader wktReader;

    @Override
    public void open(Configuration parameters) throws Exception {
      wktReader = new WKTReader();
    }

    @Override
    public void flatMap(String text, Collector<Tuple2<Boolean, Geometry>> collector) throws Exception {
      try {
        String[] list = text.split(";");
        int id = Integer.parseInt(list[0]);
        String name = list[1];
        Geometry geometry = wktReader.read(list[2]);
        geometry.setUserData(new Tuple2<>(id, name));
        collector.collect(new Tuple2<>(true, geometry));
      } catch (Exception e) {
        System.out.println("Cannot parse record: " + text);
      }
    }
  }
}