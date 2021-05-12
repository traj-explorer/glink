package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.operator.judgement.IndexKNNJudgement;
import com.github.tm.glink.core.operator.judgement.NativeKNNJudgement;
import com.github.tm.glink.features.Point;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Coordinate;

/**
 * Perform k-NN query with the points collected in a tumbling time window.
 * @author Yu Liebing
 */
public class KNNQuery {

  /**
   * @param geoDataStream The input data stream composed of {@link Point} objects.
   * @param queryPoint A coordinate to locate the query point.
   * @param k The number of points to get.
   * @param windowSize Time size of windows.
   * @param useIndex If true, Glink will use H3Index to accelerate the query.
   * @param indexRes The level of resolution to start query
   * @return Data stream containing K points.
   */
  public static DataStream<Point> pointKNNQuery(
          DataStream<Point> geoDataStream,
          Coordinate queryPoint,
          int k,
          int windowSize,
          boolean useIndex,
          int indexRes) {
    int partitionNum = 2;
    if (useIndex) {
      return geoDataStream.map(new H3IndexAssigner(indexRes))
              .keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
              .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
              .apply(new IndexKNNJudgement.IndexKeyedKNNJudgement(queryPoint, k, indexRes))
              .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
              .apply(new NativeKNNJudgement.NativeAllKNNJudgement(queryPoint, k));
    }
    return geoDataStream.keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
            .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new NativeKNNJudgement.NativeKeyedKNNJudgement(queryPoint, k))
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new NativeKNNJudgement.NativeAllKNNJudgement(queryPoint, k));
  }
}
