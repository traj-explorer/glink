package com.github.tm.glink.core.operator.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Yu Liebing
 * */
public class UVAggregateFunction<IN> implements
        AggregateFunction<IN, Tuple2<Object, Long>, Tuple2<Object, Long>> {

  @Override
  public Tuple2<Object, Long> createAccumulator() {
    return null;
  }

  @Override
  public Tuple2<Object, Long> add(IN in, Tuple2<Object, Long> objectLongTuple2) {
    return null;
  }

  @Override
  public Tuple2<Object, Long> getResult(Tuple2<Object, Long> objectLongTuple2) {
    return null;
  }

  @Override
  public Tuple2<Object, Long> merge(Tuple2<Object, Long> objectLongTuple2, Tuple2<Object, Long> acc1) {
    return null;
  }
}
