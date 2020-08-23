package com.github.tm.glink.operator.judgement;

import org.apache.flink.api.common.functions.RichFilterFunction;

import com.github.tm.glink.features.GeoObject;

import java.io.IOException;

/**
 * @author Yu Liebing
 */
public abstract class RangeJudgementBase<T extends GeoObject> extends RichFilterFunction<T> {

  public abstract boolean rangeFilter(T geoObject) throws IOException;

  @Override
  public final boolean filter(T geoObject) throws Exception {
    return rangeFilter(geoObject);
  }
}
