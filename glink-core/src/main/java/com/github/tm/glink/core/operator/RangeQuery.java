package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.operator.judgement.H3RangeJudgement;
import com.github.tm.glink.core.operator.judgement.NativeRangeJudgement;
import com.github.tm.glink.features.Point;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * Perform k-NN query with the points collected in a tumbling time window.
 * @author Yu Liebing
 */
public class RangeQuery {
  /**
   * Get the points inside the query geometry in parallel, the degree of parallelization depends
   * on the provided partition number.
   * @param geoDataStream The input point data stream.
   * @param queryGeometry A geometry, now only polygons are allowed.
   * @param partitionNum The number of partitions.
   * @param res The resolution level of H3Index.
   * @return Point data stream filtered by query geometry.
   */
  public static <U extends Geometry> DataStream<Point> spatialRangeQuery(
      DataStream<Point> geoDataStream,
      U queryGeometry,
      int partitionNum,
      int res) {
    String index = geoDataStream.getExecutionConfig().getGlobalJobParameters().toMap().get("rangeIndex");
    if (index == null || index.equals("null")) {
      return geoDataStream.filter(new NativeRangeJudgement<>(queryGeometry));
    } else if (index.equals("all")) {
      return null;
    } else if (index.equals("polygon")) {
      return geoDataStream.map(new H3IndexAssigner(res))
          .keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
          .filter(new H3RangeJudgement<>(queryGeometry, res));
    } else {
      throw new IllegalArgumentException("Unsupported `rangeIndex`, should be one of the `null`, `polygon`, `all`");
    }
  }

  public static <U extends Geometry> DataStream<Point> spatialRangeQuery(
      DataStream<Point> geoDataStream,
      Envelope queryWindow,
      int partitionNum,
      int res) {
    return geoDataStream.map(new H3IndexAssigner(res))
        .keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
        .filter(new H3RangeJudgement<>(queryWindow, res));
  }
}
