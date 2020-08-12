package com.github.tm.glink.operator.judgement;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.index.GridIndex;
import com.github.tm.glink.index.H3Index;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Yu Liebing
 */
public class IndexKNNJudgement {

  public static void windowApply(
          Coordinate queryPoint,
          int k,
          GridIndex gridIndex,
          Integer windowKey,
          TimeWindow timeWindow,
          Iterable<Point> iterable,
          Collector<Point> collector) {
    long startTime = System.currentTimeMillis();

    long queryPointIndex = gridIndex.getIndex(queryPoint.getX(), queryPoint.getY());
    // build index
    Map<Long, List<Point>> pointsInGrid = new HashMap<>();
    for (Point p : iterable) {
      long key = p.getIndex();
      List<Point> value = pointsInGrid.get(key);
      if (value == null) {
        pointsInGrid.put(key, new ArrayList<Point>() {{ add(p); }});
      } else {
        value.add(p);
      }
    }
    // search the grids
    long curIndex = queryPointIndex;
    int curRes = gridIndex.getRes();
    long curNum = 0;
    List<Long> grids = new ArrayList<>();
    grids.add(curIndex);
    while (true) {
      for (long grid : grids) {
        List<Point> points = pointsInGrid.get(grid);
        curNum += (points != null ? points.size() : 0);
      }
      if (curNum >= k || curRes <= 0) {
        break;
      }
      curIndex = gridIndex.getParent(curIndex, --curRes);
      grids.clear();
      grids.addAll(gridIndex.getChildren(curIndex, gridIndex.getRes()));
    }
    // collect final points
    List<Long> curGrids = gridIndex.kRing(curIndex, 2);
    int totalGrids = 0;
    for (long grid : curGrids) {
      List<Long> finalGrids = gridIndex.getChildren(grid, gridIndex.getRes());
      totalGrids += finalGrids.size();
      for (long g : finalGrids) {
        List<Point> points = pointsInGrid.get(g);
        if (points != null) {
          for (Point p : points) {
            collector.collect(p);
          }
        }
      }
    }

    long endTime = System.currentTimeMillis();
    // log information for debug
    System.out.println(String.format("ThreadId: %d, key: %d, total grids: %d, time: %d",
            Thread.currentThread().getId(), windowKey, totalGrids, (endTime - startTime)));
  }

  public static class IndexKeyedKNNJudgement extends RichWindowFunction<Point, Point, Integer, TimeWindow> {
    private static Logger logger = LoggerFactory.getLogger(IndexKeyedKNNJudgement.class);

    private Coordinate queryPoint;
    private int k;
    private transient GridIndex gridIndex;
    private int indexRes;

    public IndexKeyedKNNJudgement(Coordinate queryPoint, int k, int indexRes) {
      this.queryPoint = queryPoint;
      this.k = k;
      this.indexRes = indexRes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      gridIndex = new H3Index(indexRes);
    }

    @Override
    public void apply(Integer windowKey, TimeWindow timeWindow, Iterable<Point> iterable, Collector<Point> collector)
            throws Exception {
      windowApply(queryPoint, k, gridIndex, windowKey, timeWindow, iterable, collector);
    }
  }

  public static class IndexAllKNNJudgement extends RichAllWindowFunction<Point, Point, TimeWindow> {

    private Coordinate queryPoint;
    private int k;
    private transient GridIndex gridIndex;
    private int indexRes;

    public IndexAllKNNJudgement(Coordinate queryPoint, int k, int indexRes) {
      this.queryPoint = queryPoint;
      this.k = k;
      this.indexRes = indexRes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      this.gridIndex = new H3Index(indexRes);
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Point> iterable, Collector<Point> collector) throws Exception {
      windowApply(queryPoint, k, gridIndex, null, timeWindow, iterable, collector);
    }
  }

}
