package com.github.tm.glink.traffic;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.utils.GeoUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class RoadSpeedOp extends RichWindowFunction<TrajectoryPoint, Tuple2<Long, Double>, Long, TimeWindow> {

  @Override
  public void apply(Long roadId,
                    TimeWindow timeWindow,
                    Iterable<TrajectoryPoint> iterable,
                    Collector<Tuple2<Long, Double>> collector) throws Exception {
    long windowInterval = (timeWindow.getEnd() - timeWindow.getStart()) / 1000;
    Map<String, List<TrajectoryPoint>> points = new HashMap<>();
    for (TrajectoryPoint p : iterable) {
      points.putIfAbsent(p.getId(), new ArrayList<>());
      points.get(p.getId()).add(p);
    }

    double avgSpeed = 0.d;
    int validCarNum = 0;
    for (Map.Entry<String, List<TrajectoryPoint>> e : points.entrySet()) {
      List<TrajectoryPoint> carPoints = e.getValue();
      if (carPoints.size() <= 1) continue;

      ++validCarNum;
      double dis = 0.d;
      long timeInterval = 0L;
      for (int i = 1, len = carPoints.size(); i < len; ++i) {
        dis += GeoUtil.computeGeoDistance(carPoints.get(i - 1), carPoints.get(i));
        timeInterval += carPoints.get(i).getTimestamp() - carPoints.get(i - 1).getTimestamp();
      }
      double carSpeed = dis / (timeInterval / 1000);
      avgSpeed += carSpeed;
    }
    if (validCarNum == 0) return;
    avgSpeed /= validCarNum;
    collector.collect(new Tuple2<>(roadId, avgSpeed));
  }
}
