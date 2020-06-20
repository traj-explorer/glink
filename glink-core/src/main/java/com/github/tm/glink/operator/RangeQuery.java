package com.github.tm.glink.operator;

import com.github.tm.glink.feature.GeoObject;
import com.github.tm.glink.operator.judgement.NativeRangeJudgement;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public class RangeQuery {

  public static <T extends GeoObject, U extends Geometry> DataStream<T> spatialRangeQuery(
          DataStream<T> geoDataStream,
          U queryGeometry) {
    String index = geoDataStream.getExecutionConfig().getGlobalJobParameters().toMap().get("rangeIndex");
    if (index == null || index.equals("null")) {
      return geoDataStream.filter(new NativeRangeJudgement<>(queryGeometry));
    } else if (index.equals("all")) {
      return null;
    } else if (index.equals("polygon")) {
      return null;
    } else {
      throw new IllegalArgumentException("Unsupported `rangeIndex`, should be one of the `null`, `polygon`, `all`");
    }
  }

  public static <T extends GeoObject, U extends Geometry> DataStream<T> spatialRangeQuery(
          DataStream<T> geoDataStream,
          Envelope queryWindow) {
    return geoDataStream.filter(new NativeRangeJudgement<>(queryWindow));
  }
}
