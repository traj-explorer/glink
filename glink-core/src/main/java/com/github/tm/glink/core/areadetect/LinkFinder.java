package com.github.tm.glink.core.areadetect;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

public  class LinkFinder<T> extends ProcessWindowFunction<Tuple3<Long, DetectionUnit<T>, Long>, Tuple2<Long, Long>, Long, TimeWindow> {

  @Override
  public void process(Long borderID, Context context, Iterable<Tuple3<Long, DetectionUnit<T>, Long>> iterable, Collector<Tuple2<Long, Long>> collector) throws Exception {
    HashMap<DetectionUnit<T>, Long> lowerSet = new HashMap<>();
    HashMap<DetectionUnit<T>, Long> upperSet = new HashMap<>();
    HashSet<LocalAreaConnectPair> pairs = new HashSet<>();
    // 这里涉及到局部区域ID的生成方式和边界区域ID的生成方式，参加LocalDetection类。
    for (Tuple3<Long, DetectionUnit<T>, Long> t : iterable) {
      long partition = t.f2 >> 32;
      if (partition == getLowerPartitionID(borderID))
        lowerSet.put(t.f1, t.f2);
      else
        upperSet.put(t.f1, t.f2);
    }
    for (Map.Entry<DetectionUnit<T>, Long> entry : lowerSet.entrySet()) {
      DetectionUnit<T> unit1 = entry.getKey();
      Long localAreaID1 = entry.getValue();
      if (upperSet.containsKey(unit1)) {
        Long localAreaID2 = upperSet.get(unit1);
        if (!Objects.equals(localAreaID1, localAreaID2)) {
          LocalAreaConnectPair pair = new LocalAreaConnectPair(localAreaID1, localAreaID2);
          if (!pairs.contains(pair)) {
            collector.collect(new Tuple2<>(pair.localAreaID1, pair.localAreaID2));
            pairs.add(pair);
          }
        }
      }
    }
  }

  private long getLowerPartitionID(long id) {
    return id >> 32;
  }

  private static class LocalAreaConnectPair {
    long localAreaID1;
    long localAreaID2;

    LocalAreaConnectPair(Long partition1, Long partition2) {
      this.localAreaID1 = Math.min(partition1, partition2);
      this.localAreaID2 = Math.max(partition1, partition2);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LocalAreaConnectPair that = (LocalAreaConnectPair) o;
      return (localAreaID1 == that.localAreaID1 && localAreaID2 == that.localAreaID2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(localAreaID1, localAreaID2);
    }
  }
}
