package com.github.tm.glink.examples.query;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.format.Schema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class RangeQueryExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    SpatialDataStream<Point> pointSpatialDataStream = new SpatialDataStream<>(
            env, 0, 1, TextFileSplitter.COMMA, GeometryType.POINT, true,
            Schema.types(Integer.class, String.class),
            "22.3,33.4,1,hangzhou", "22.4,33.6,2,wuhan");

    // create query polygons
    WKTReader wktReader = new WKTReader();
    Polygon queryPolygon1 = (Polygon) wktReader.read("POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))");
    List<Polygon> queryPolygons = new ArrayList<>();
    queryPolygons.add(queryPolygon1);

    SpatialDataStream<Point> s = pointSpatialDataStream.index().rangeQuery(queryPolygons);
    s.print();

    env.execute();
  }
}
