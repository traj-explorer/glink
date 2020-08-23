package com.github.tm.glink.operator.process;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.github.tm.glink.features.Point;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;


public class DbscanExpandCluster extends ProcessAllWindowFunction<Tuple2<Point, Point>, List<Point>, TimeWindow> {
  private HashMap<Point, Integer> occurCounter;
  private List<Point> noisePoints;
  private List<Point> corePoints;
  private Integer minPts;

  /**
   * 对时间窗口内接收到的Neighbor stream进行Dbscan操作，以List形式输出各个Cluster.
   * @param minPts 一个cluster中至少应包含的点的数量。
   */
  public DbscanExpandCluster(Integer minPts) {
    this.minPts = minPts;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    occurCounter = new HashMap<>();
    noisePoints = new LinkedList<>();
    corePoints = new LinkedList<>();
  }

  @SuppressWarnings({"checkstyle:RegexpSingleline", "checkstyle:EmptyBlock"})
  @Override
  public void process(Context context, Iterable<Tuple2<Point, Point>> iterable, Collector<List<Point>> collector) throws Exception {
    for (Tuple2<Point, Point> temp : iterable) {
      addToCounter(temp);
    }
    Set<Point> set = occurCounter.keySet();
    for (Point point : set) {
      // 若已经在之前已经被设定为core point/noise point，暂不考虑这个点。
      if (corePoints.contains(point) || noisePoints.contains(point)) {
        // do nothing.
      } else if (occurCounter.get(point) >= minPts) {
        // 该点为一个core point，由该点起始，寻找它所处的整个cluster。
        corePoints.add(point);
        List<Point> cluster = new LinkedList<>();
        cluster.add(point);
        Stack<Point> seeds = getInitSeeds(getAllNeighborPoints(point, iterable));
        Iterator<Point> iterator = seeds.iterator();
        while (!seeds.empty()) {
          Point seed = seeds.pop();
          cluster.add(seed);
          if (occurCounter.get(seed) >= minPts) {
            corePoints.add(seed);
            for (Point neighbor : getAllNeighborPoints(seed, iterable)) {
              if (!seeds.contains(neighbor) && (!corePoints.contains(neighbor) || noisePoints.contains(neighbor))) {
                seeds.push(neighbor);
              }
            }
          } else {
            noisePoints.add(point);
          }
        }
        collector.collect(cluster);
      } else {
        noisePoints.add(point);
      }
    }
    FileWriter fw = new FileWriter("/Users/haocheng/Desktop/glink/glink-examples/src/main/resources/cluster_result.txt", true);
    for (Point point: noisePoints) {
      fw.write(point.getLat() + "," + point.getLng() + "," + -1);
      fw.write("\n");
    }
    fw.close();
  }

  private void addToCounter(Tuple2<Point, Point> in) {
    if (occurCounter.containsKey(in.f0)) {
      occurCounter.put(in.f0, occurCounter.get(in.f0) + 1);
    } else {
      occurCounter.put(in.f0, 1);
    }
    if (occurCounter.containsKey(in.f1)) {
      occurCounter.put(in.f1, occurCounter.get(in.f1) + 1);
    } else {
      occurCounter.put(in.f1, 1);
    }
  }

  private Stack<Point> getInitSeeds(List<Point> list) {
    Stack<Point> stack = new Stack<>();
    for (Point point : list) {
      stack.push(point);
    }
    return stack;
  }

  private List<Point> getAllNeighborPoints(Point in, Iterable<Tuple2<Point, Point>> iterable) {
    Iterator<Tuple2<Point, Point>> iterator = iterable.iterator();
    List<Point> ret = new LinkedList<>();
    Tuple2<Point, Point> temp;
    while (iterator.hasNext()) {
      temp = iterator.next();
      if (temp.f0.equals(in)) {
        ret.add(temp.f1);
      } else if (temp.f1.equals(in)) {
        ret.add(temp.f0);
      }
    }
    return ret;
  }
}

