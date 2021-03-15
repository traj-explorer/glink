package com.github.tm.glink.examples.query;

import com.github.tm.glink.examples.source.CSVDiDiGPSPointSource;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.operator.RangeQuery;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

import java.time.Duration;

/**
 * @author Yu Liebing
 */
public class RangeQueryJob {

  /**
   * --rangeIndex Whether to assign indexes for the query conditions.
   * "null" Do not assign any index.
   * "all" Assign indexes for all of the query conditions.
   * "polygon" Only assign indexes for the polygon query conditions.
   */
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool globalParam = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(globalParam);

//    String path = KNNQueryJob.class.getResource("/gps_20161101_0710").getPath();
    String path = "/home/liebing/input/gps_20161101_0710";
    DataStream<Point> pointDataStream = env.addSource(new CSVDiDiGPSPointSource(path))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp)->event.getTimestamp()));

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