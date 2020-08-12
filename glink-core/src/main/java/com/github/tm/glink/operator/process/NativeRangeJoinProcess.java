package com.github.tm.glink.operator.process;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.fearures.utils.GeoUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class NativeRangeJoinProcess
        implements WindowFunction<Tuple3<Integer, Boolean, Point>, List<Point>, Integer, TimeWindow> {

  private double distance;

  public NativeRangeJoinProcess(double distance) {
    this.distance = distance;
  }

  @SuppressWarnings("checkstyle:NeedBraces")
  @Override
  public void apply(Integer key,
                    TimeWindow timeWindow,
                    Iterable<Tuple3<Integer, Boolean, Point>> iterable,
                    Collector<List<Point>> collector)
          throws Exception {
    List<Tuple3<Integer, Boolean, Point>> windowPoints = new ArrayList<>();
    for (Tuple3<Integer, Boolean, Point> t : iterable) {
      windowPoints.add(t);
    }

    for (int i = 0, len = windowPoints.size(); i < len; ++i) {
      if (!windowPoints.get(i).f1) continue;
      List<Point> nearPoints = new ArrayList<>();
      nearPoints.add(windowPoints.get(i).f2);
      for (int j = 0; j < len; ++j) {
        if (j == i) continue;
        double dis = GeoUtil.computeGeoDistance(windowPoints.get(i).f2, windowPoints.get(j).f2);
        if (dis < distance) nearPoints.add(windowPoints.get(j).f2);
      }
      collector.collect(nearPoints);
    }
  }
}
