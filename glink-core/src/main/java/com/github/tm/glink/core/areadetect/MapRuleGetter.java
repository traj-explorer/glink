package com.github.tm.glink.core.areadetect;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MapRuleGetter extends ProcessAllWindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, TimeWindow> {
  @Override
  public void process(Context context, Iterable<Tuple2<Long, Long>> elements, Collector<Tuple2<Long, Long>> out) throws Exception {
    HashMap<Long, Integer> areaIDtoIndex = new HashMap<>();
    List<List<Long>> l = new LinkedList<>();
    for (Tuple2<Long, Long> t : elements) {  // 1. 两者均出现过, 移动到小的索引中。
      if (areaIDtoIndex.containsKey(t.f0) && areaIDtoIndex.containsKey(t.f1)) {
        int index0 = areaIDtoIndex.get(t.f0);
        int index1 = areaIDtoIndex.get(t.f1);
        if (index0 != index1) {
          List<Long> toMove = l.get(Math.max(index0, index1));
          int targetIndex = Math.min(index0, index1);
          for (Long movedAreaIDs : toMove) {
            areaIDtoIndex.put(movedAreaIDs, targetIndex);
          }
          l.get(targetIndex).addAll(toMove);
          toMove.clear();
        }
      } else if (areaIDtoIndex.containsKey(t.f0) || areaIDtoIndex.containsKey(t.f1)) { // 2. 两者出现过其中一个，把未出现过的放到已经出现过了的位置上。
        if (areaIDtoIndex.containsKey(t.f0)) {
          int targetIndex = areaIDtoIndex.get(t.f0);
          l.get(targetIndex).add(t.f1);
          areaIDtoIndex.put(t.f1, targetIndex);
        } else {
          int targetIndex = areaIDtoIndex.get(t.f1);
          l.get(targetIndex).add(t.f0);
          areaIDtoIndex.put(t.f0, targetIndex);
        }
      } // 3. 两者都没有出现过
      else {
        List<Long> toAdd = new LinkedList<>();
        toAdd.add(t.f0); toAdd.add(t.f1);
        areaIDtoIndex.put(t.f0, l.size());
        areaIDtoIndex.put(t.f1, l.size());
        l.add(l.size(), toAdd);
      }
    }
    // 为每组分配global id。
    long globalID = 0;
    for (List<Long> list : l) {
      for (Long localID : list) {
        out.collect(new Tuple2<>(localID, globalID));
      }
      globalID++;
    }
  }
}