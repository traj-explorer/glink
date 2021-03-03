package com.github.tm.glink.examples.query;

import com.github.tm.glink.examples.source.CSVDiDiGPSPointSource;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.operator.KNNQuery;
import com.github.tm.glink.features.TemporalObject;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.locationtech.jts.geom.Coordinate;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * @author Yu Liebing
 */
public class KNNQueryJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);

    int k = 10;
    Coordinate queryPoint = new Coordinate(30.66, 104.05);

    String path = "/home/liebing/input/gps_20161101_0710";
    DataStream<Point> pointDataStream = env.addSource(new CSVDiDiGPSPointSource(path))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp)->event.getTimestamp()));


    System.out.println("source");

    DataStream<Point> knnResult = KNNQuery.pointKNNQuery(
            pointDataStream, queryPoint, k, 50, false, 9);
    knnResult.print();

    env.execute("KNN Query test");
    System.out.println("execute");
  }

//  public static class EventTimeAssigner implements AssignerWithPeriodicWatermarks<TemporalObject> {
//
//    private final long maxOutOfOrderness;
//    private long currentMaxTimestamp;
//
//    public EventTimeAssigner(long maxOutOfOrderness) {
//      this.maxOutOfOrderness  = maxOutOfOrderness;
//    }
//
//    @Nullable
//    @Override
//    public Watermark getCurrentWatermark() {
//      return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
//    }
//  }
}
