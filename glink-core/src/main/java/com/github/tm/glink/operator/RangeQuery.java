package com.github.tm.glink.operator;

import com.github.tm.glink.feature.GeoObject;
import com.github.tm.glink.feature.Point;
import com.github.tm.glink.operator.judgement.H3RangeJudgement;
import com.github.tm.glink.operator.judgement.NativeRangeJudgement;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public class RangeQuery {

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
      return geoDataStream.map(new IndexAssigner(res))
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
    return geoDataStream.map(new IndexAssigner(res))
        .keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
        .filter(new H3RangeJudgement<>(queryWindow, res));
  }
}
