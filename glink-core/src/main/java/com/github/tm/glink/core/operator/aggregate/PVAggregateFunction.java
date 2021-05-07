package com.github.tm.glink.core.operator.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Yu Liebing
 * */
public class PVAggregateFunction<IN> implements
        AggregateFunction<IN, Tuple2<Object, Long>, Tuple2<Object, Long>> {

  private final MapFunction<IN, Object> outMapper;

  public PVAggregateFunction(MapFunction<IN, Object> outMapper) {
    this.outMapper = outMapper;
  }

  @Override
  public Tuple2<Object, Long> createAccumulator() {
    return new Tuple2<>();
  }

  @Override
  public Tuple2<Object, Long> add(IN in, Tuple2<Object, Long> acc) {
    if (acc.f0 == null) {
      try {
        acc.f0 = outMapper.map(in);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (acc.f1 == null) {
      acc.f1 = 0L;
    }
    acc.f1 += 1;
    return acc;
  }

  @Override
  public Tuple2<Object, Long> getResult(Tuple2<Object, Long> acc) {
    return acc;
  }

  @Override
  public Tuple2<Object, Long> merge(Tuple2<Object, Long> acc1, Tuple2<Object, Long> acc2) {
    return new Tuple2<>(acc1.f0, acc1.f1 + acc2.f1);
  }
}
