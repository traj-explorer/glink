package com.github.stc.glink.operator;

import com.github.stc.glink.feature.GeoObject;
import com.github.stc.glink.feature.SpatialEnvelope;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author Yu Liebing
 */
public class RangeQuery {

  public static <T extends GeoObject> DataStream<T> spatialRangeQuery(
          DataStream<T> geoDataStream, SpatialEnvelope envelope) {
    return geoDataStream.filter(new FilterFunction<T>() {
      @Override
      public boolean filter(T t) throws Exception {
        return envelope.contain(t);
      }
    });
  }
}
