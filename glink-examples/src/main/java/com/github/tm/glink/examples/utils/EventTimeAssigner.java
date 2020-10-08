package com.github.tm.glink.examples.utils;

import com.github.tm.glink.features.Point;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author Yu Liebing
 */
public class EventTimeAssigner<T extends Point> implements AssignerWithPeriodicWatermarks<T> {

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
