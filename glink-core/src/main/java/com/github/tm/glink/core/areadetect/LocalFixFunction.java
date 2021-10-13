package com.github.tm.glink.core.areadetect;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class LocalFixFunction<T> extends ProcessJoinFunction<Tuple2<Long, Long>, Tuple2<Long, List<DetectionUnit<T>>>, Tuple2<Long, List<DetectionUnit<T>>>> {

  @Override
  public void processElement(Tuple2<Long, Long> left, Tuple2<Long, List<DetectionUnit<T>>> right, Context ctx, Collector<Tuple2<Long, List<DetectionUnit<T>>>> out) throws Exception {
    right.f0 = left.f1;
    out.collect(right);
  }
}