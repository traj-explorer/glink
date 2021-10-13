package com.github.tm.glink.core.areadetect.unigrid;

import com.github.tm.glink.core.areadetect.AreaDetect;
import com.github.tm.glink.core.areadetect.DetectionUnit;
import com.github.tm.glink.core.areadetect.LocalDetect;
import com.github.tm.glink.core.index.GeographicalGridIndex;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

public class UAreaDetect<T> extends AreaDetect<T> {

  GeographicalGridIndex index;

  public UAreaDetect(StreamExecutionEnvironment env,
                     TumblingEventTimeWindows windowAssigner,
                     ProcessWindowFunction<DetectionUnit<T>, DetectionUnit<T>, Long, TimeWindow> interest,
                     SingleOutputStreamOperator<DetectionUnit<T>> source,
                     GeographicalGridIndex index) {
    super(env, windowAssigner, interest, source);
    this.index = index;
  }

  @Override
  public void initRouter() {
    router = new UnigridRouter<T>(1, index);
  }

  @Override
  public void initLocalDetectFunc() {
    localDetectFunction = new UnigridLocalDetect<>(index, 4);
  }

  @Override
  public void initPolygonGetFunc() {
    polygonGetFunc = new UPolygonGetFunc<>(index);
  }

  public class UnigridRouter<T> implements FlatMapFunction<DetectionUnit<T>, Tuple2<Long, DetectionUnit<T>>> {

    int level;
    GeographicalGridIndex index;

    public UnigridRouter(int level, GeographicalGridIndex geographicalGridIndex) {
      this.level = level;
      this.index = geographicalGridIndex;
    }

    @Override
    public void flatMap(DetectionUnit<T> unigridUnit, Collector<Tuple2<Long, DetectionUnit<T>>> collector) throws Exception {
      List<Long> neighbors = index.getNeighbors(unigridUnit.getId(), 4);
      // 发向自己的分区
      Long localPartition = index.getPartition(unigridUnit.getId(), level);
      unigridUnit.setPartition(localPartition);
      collector.collect(new Tuple2<>(localPartition, unigridUnit));
      // 周边分区
      for (Long neighbor : neighbors) {
        Long neighborPartition = index.getPartition(neighbor, level);
        if (neighborPartition != localPartition) {
          collector.collect(new Tuple2<>(neighborPartition, unigridUnit));
        }
      }
    }
  }

  public static class UnigridLocalDetect<T> extends LocalDetect<T> {
    GeographicalGridIndex index;
    int type;

    public UnigridLocalDetect(GeographicalGridIndex index, int type) {
      this.index = index;
      this.type = type;
    }

    @Override
    protected List<Long> getNeighbors(DetectionUnit<T> unit) {
      LinkedList<Long> longs = new LinkedList<>(index.getNeighbors(unit.getId(), type));
      return longs;
    }
  }

}
