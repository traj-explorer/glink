package com.github.tm.glink.core.operator.process;

import com.github.tm.glink.core.index.GeographicalGridIndex;
import com.github.tm.glink.core.index.GridIndex;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class IntersectIndexAssigner<T extends Geometry> extends RichFlatMapFunction<T, Tuple2<T, List<T>>> {

  private double gridWidth;
  private transient GridIndex gridIndex;

  public IntersectIndexAssigner(double gridWidth) {
    this.gridWidth = gridWidth;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    gridIndex = new GeographicalGridIndex(gridWidth);
  }

  @Override
  public void flatMap(T t, Collector<Tuple2<T, List<T>>> collector) throws Exception {
    List<Long> indexes = gridIndex.getIntersectIndex(t);

  }
}
