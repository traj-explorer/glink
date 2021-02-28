package com.github.tm.glink.traffic;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.mapmathcing.MapMatcher;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

public class RoadSpeedPipeline {

  public static DataStream<Tuple2<Long, Double>> roadSpeedPipeline(
          DataStream<TrajectoryPoint> trajectoryPointDataStream,
          int windowSize,
          int windowSlide) {
    DataStream<TrajectoryPoint> matchedStream = MapMatcher.mapMatch(trajectoryPointDataStream);
    return matchedStream
            .assignTimestampsAndWatermarks(new EventTimeAssigner<>(5))
            .keyBy(r -> (Long) r.getAttributes().get("roadId"))
            .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlide)))
            .apply(new RoadSpeedOp());
  }

  public static class EventTimeAssigner<T extends Point> implements AssignerWithPeriodicWatermarks<T> {

    private final long maxOutOfOrderness;
    private long currentMaxTimestamp;

    public EventTimeAssigner(long maxOutOfOrderness) {
      this.maxOutOfOrderness  = maxOutOfOrderness;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
      return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(T p, long l) {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, p.getTimestamp());
      return p.getTimestamp();
    }
  }
}
