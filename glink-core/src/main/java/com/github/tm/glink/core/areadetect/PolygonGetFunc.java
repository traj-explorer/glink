package com.github.tm.glink.core.areadetect;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.util.LinkedList;
import java.util.List;

public abstract class PolygonGetFunc<T> extends ProcessWindowFunction<Tuple2<Long, List<DetectionUnit<T>>>, Geometry, Long, TimeWindow> {

  @Override
  public void process(Long aLong, Context context, Iterable<Tuple2<Long, List<DetectionUnit<T>>>> elements, Collector<Geometry> out) throws Exception {
    LinkedList<Polygon> list = new LinkedList<>();
    for (Tuple2<Long, List<DetectionUnit<T>>> element : elements) {
      Polygon[] polygons = new Polygon[element.f1.size()];
      int i = 0;
      for (DetectionUnit<T> detectionUnit : element.f1) {
        polygons[i] = getUnitPolygon(detectionUnit.getId());
        i++;
      }
      MultiPolygon mp = new MultiPolygon(polygons, new GeometryFactory());
      list.add((Polygon) mp.union());
    }
    Polygon[] ps = new Polygon[list.size()];
    for (int i = 0; i < ps.length; i++) {
      ps[i] = list.get(i);
    }
    MultiPolygon res = new MultiPolygon(ps, new GeometryFactory());
    out.collect(res.union());
  }

  protected abstract Polygon getUnitPolygon(Long id);

}
