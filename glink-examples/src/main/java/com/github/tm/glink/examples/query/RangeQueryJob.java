package com.github.tm.glink.examples.query;

import com.github.tm.glink.examples.source.CSVDiDiGPSPointSource;
import com.github.tm.glink.feature.Point;
import com.github.tm.glink.operator.RangeQuery;
import com.github.tm.glink.source.CSVPointSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Yu Liebing
 */
public class RangeQueryJob {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, String> param = new HashMap<>();
    param.put("rangeIndex", "polygon");
    ParameterTool globalParam = ParameterTool.fromMap(param);
    env.getConfig().setGlobalJobParameters(globalParam);

    String path = KNNQueryJob.class.getResource("/gps_20161101_0710").getPath();
    DataStream<Point> pointDataStream = env.addSource(new CSVDiDiGPSPointSource(path))
        .assignTimestampsAndWatermarks(new KNNQueryJob.EventTimeAssigner(5000));

    Coordinate[] coorArray = new Coordinate[]{new Coordinate(30.5, 104),
        new Coordinate(30.8, 104),
        new Coordinate(30.8, 104.5),
        new Coordinate(30.5, 104.5),
        new Coordinate(30.5, 104)};

    Polygon queryRange = new Polygon(new LinearRing(coorArray, new PrecisionModel(), 4326), null, new GeometryFactory(new PrecisionModel(), 4326));

    DataStream<Point> rangeQueryStream = RangeQuery.spatialRangeQuery(pointDataStream, queryRange, 2, 5);

    rangeQueryStream.print();

    env.execute("test");
  }
}