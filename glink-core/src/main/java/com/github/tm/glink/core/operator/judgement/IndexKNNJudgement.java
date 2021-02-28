package com.github.tm.glink.core.operator.judgement;

import com.github.tm.glink.core.index.H3Index;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.index.GridIndex;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Yu Liebing
 */
public class IndexKNNJudgement {

  /**
   * Apply KNN on the collected points.
   * @param queryPoint Query point's coordinate.
   * @param k Parameter "k" in kNN.
   * @param gridIndex The type of index used in query.
   * @param windowKey The index of the points collected by current window. If assigned,
   * @param timeWindow A timewindow object provided by window function.
   * @param iterable Used to traverse the points inside the window.
   * @param collector Used to collect results.
   */
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
    List<Long> outestGrids = new ArrayList<>();
    List<Long> confirmedGrids = new ArrayList<>();
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
//    int kring = 1;
//    while (true) {
//      for (long grid : grids) {
//        List<Point> points = pointsInGrid.get(grid);
//        if(points!=null) {
//          curNum += points.size();
//        }
//      }
//      if (curNum >= k) {
//        break;
//      } else {
//        // 外扩一层, 当外扩失败时,已经为最大时, break, 输出已有点。
//        List<Long> toAddGrids = gridIndex.kRing(curIndex, ++kring);
//        confirmedGrids.clear();
//        confirmedGrids.addAll(grids);
//        boolean expand_success = true;
//        // 设置接下来需要遍历的grids与outest grids.
//        if (expand_success) {
//          List<Long> tempGrids = new ArrayList<>();
//          for (Long toAddGrid : toAddGrids) {
//            if(!grids.contains(toAddGrid)) {
//              tempGrids.add(toAddGrid);
//            }
//          }
//          grids.clear();
//          grids.addAll(tempGrids);
//          outestGrids.clear();
//          outestGrids.addAll(tempGrids);
//        } else {
//          System.out.println("Didn't get k points.");
//          break;
//        }
//      }
//    }
//    int totalGrids = 0;
//    int current_points = 0;
//    for ( Long confirmedGrid : confirmedGrids) {
//      totalGrids ++;
//      List<Point> points = pointsInGrid.get(confirmedGrid);
//      for( Point point : points){
//        collector.collect(point);
//        current_points++;
//      }
//    }
//    PriorityQueue<Point> priorityQueue = new PriorityQueue<>(new GeoDistanceComparator(queryPoint));
//    for ( Long outGrids : outestGrids) {
//      totalGrids ++;
//      priorityQueue.addAll(pointsInGrid.get(outGrids));
//    }
//    priorityQueue.
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
