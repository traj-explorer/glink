package com.github.tm.glink.operator;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.operator.process.AllIndexRangeJoinProcess;
import com.github.tm.glink.operator.process.RangeJoinIndexAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class AllRangeJoin {
  public static DataStream<List<Point>> allRangeJoin(
          DataStream<Point> geoDataStream,
          int windowSize,
          double distance,
          double gridWidth) {
    return geoDataStream
            .flatMap(new RangeJoinIndexAssigner(distance, gridWidth, true))
            .keyBy(p -> p.f1)
            .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new AllIndexRangeJoinProcess(distance));
  }
}
