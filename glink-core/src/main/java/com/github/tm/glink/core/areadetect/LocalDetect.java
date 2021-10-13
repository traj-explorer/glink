package com.github.tm.glink.core.areadetect;

import com.github.tm.glink.core.index.GeographicalGridIndex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;


public abstract class LocalDetect<T> extends ProcessWindowFunction<Tuple2<Long, DetectionUnit<T>>, Tuple2<Long, List<DetectionUnit<T>>>, Long, TimeWindow> {

  // 边界区域ID，检测单元信息，局部区域ID。
  OutputTag<Tuple3<Long, DetectionUnit<T>, Long>> borderUnits;
  // 局部区域ID，局部区域中的全部检测单元。
  OutputTag<Tuple2<Long, List<DetectionUnit<T>>>> needCombineTag;


  @Override
  public void open(Configuration parameters) throws Exception {
    borderUnits = new OutputTag<Tuple3<Long, DetectionUnit<T>, Long>>("border") { };
    needCombineTag = new OutputTag<Tuple2<Long, List<DetectionUnit<T>>>>("need combine") { };
    super.open(parameters);
  }

  @Override
  public void process(Long key, Context context, Iterable<Tuple2<Long, DetectionUnit<T>>> iterable, Collector<Tuple2<Long, List<DetectionUnit<T>>>> out) throws Exception {
    HashMap<Long, DetectionUnit<T>> visited = new HashMap<>();
    HashMap<Long, DetectionUnit<T>> units = new HashMap<>();
    int count = 0;
    // 转换成id -> 单元，方便查询
    for (Tuple2<Long, DetectionUnit<T>> unit : iterable) {
      units.put(unit.f1.getId(), unit.f1);
    }
    for (DetectionUnit<T> unit : units.values()) {
      if (visited.containsKey(unit.getId()) || !Objects.equals(unit.getPartition(), key))
        continue;
      visited.put(unit.getId(), unit);
      // -----------  a new local area id;
      count++;
      Long currAreaID = getLocalAreaID(key, count);
      // ----------- flag indicating to sink or fix。
      boolean needCombine = false;
      // list to collect the data in the local area;
      List<DetectionUnit<T>> uninList = new ArrayList<>();
      // do dfs
      Stack<DetectionUnit<T>> stack = new Stack<>();
      stack.add(unit);
      while (!stack.isEmpty()) {
        // 栈中的元素一定在这个分区内
        DetectionUnit<T> temp = stack.pop();
        uninList.add(temp);
        List<Long> neighborIDs = getNeighbors(temp);
        List<DetectionUnit<T>> neighbors = new LinkedList<>();
        for (Long id : neighborIDs) {
          if (units.containsKey(id)) {
            neighbors.add(units.get(id));
          }
        }
        for (DetectionUnit<T> neighbor : neighbors) {
          if (!visited.containsKey(neighbor.getId())) {
            visited.put(neighbor.getId(), neighbor);
            Long partitionOfNeighbor = neighbor.getPartition();
            if (!Objects.equals(partitionOfNeighbor, key)) { // 如果栈中一个点的邻居在分区外，说明这个点本身在边界上，需要把他的邻居发向下游的边界上。
              needCombine = true;
              context.output(borderUnits, new Tuple3<>(getBorderID(key, partitionOfNeighbor), temp, currAreaID));
              context.output(borderUnits, new Tuple3<>(getBorderID(key, partitionOfNeighbor), neighbor, currAreaID));
            } else {
              stack.push(neighbor);
            }
          }
        }
      }
      if (!needCombine) {
        out.collect(new Tuple2<>(currAreaID, uninList));
      } else {
        context.output(needCombineTag, new Tuple2<>(currAreaID, uninList));
      }
    }
  }

  /**
   * 更小的分区id在左侧高位，更低的分区id在低位
   */
  private long getBorderID(long a, long b) {
    if (a > b) {
      return b << 32 | a;
    } else {
      return a << 32 | b;
    }
  }

  protected abstract List<Long> getNeighbors(DetectionUnit<T> unit);

  private Long getLocalAreaID(Long partitionID, int count) {
    return partitionID << 32 | count;
  }
}