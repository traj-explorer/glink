package com.github.tm.glink.operator;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.operator.process.PairIndexRangeJoinProcess;
import com.github.tm.glink.operator.process.RangeJoinIndexAssigner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Yu Liebing
 */
public class PairRangeJoin {
  public static DataStream<Tuple2<Point, Point>> pairRangeJoin(
          DataStream<Point> geoDataStream,
          int windowSize,
          double distance,
          double gridWidth) {
    return geoDataStream
            .flatMap(new RangeJoinIndexAssigner(distance, gridWidth, false))
            .keyBy(p -> p.f1.getIndex())
            .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new PairIndexRangeJoinProcess(distance));
  }
}
