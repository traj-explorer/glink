package com.github.tm.glink.benchmark.tdrive;

import com.github.tm.glink.core.geom.MultiPolygonWithIndex;
import com.github.tm.glink.core.geom.PolygonWithIndex;
import com.github.tm.glink.core.index.TRTreeIndex;
import com.github.tm.glink.core.index.TreeIndex;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Yu Liebing
 * */
public class ContainsBenchmark {

  @State(Scope.Thread)
  public static class BenchmarkState {

    private List<Point> points;
    private List<Geometry> districts;
    private TreeIndex<Geometry> treeIndex;
    private TreeIndex<Geometry> indexedPolygonTree;
    private int count;

    @Setup
    public void setUp() {
      // /media/liebing/p/data/beijing_district/beijing_district.csv
      districts = Utils.readDistrict("/media/liebing/p/data/beijing_district/beijing_district.csv");

      treeIndex = new TRTreeIndex<>();
      treeIndex.insert(districts);

      indexedPolygonTree = new TRTreeIndex<>();
      districts.forEach(geom -> {
        if (geom instanceof Polygon) {
          indexedPolygonTree.insert(PolygonWithIndex.fromPolygon((Polygon) geom));
        } else if (geom instanceof MultiPolygon) {
          indexedPolygonTree.insert(MultiPolygonWithIndex.fromMultiPolygon((MultiPolygon) geom));
        } else {
          indexedPolygonTree.insert(geom);
        }
      });

      points = Utils.readTrajectory("/media/liebing/p/data/T-drive/release/tdrive_merge.txt");
      count = 100000;
    }
  }

  @Benchmark
  @BenchmarkMode({Mode.AverageTime})
  public void traversalBenchmark(BenchmarkState state) {
    for (int i = 0; i < state.count; ++i) {
      List<Geometry> result = new ArrayList<>();
      for (Geometry g : state.districts) {
        if (g.contains(state.points.get(i))) {
          result.add(g);
        }
      }
    }
  }

  @Benchmark
  @BenchmarkMode({Mode.AverageTime})
  public void rTreeBenchmark(BenchmarkState state) {
    for (int i = 0; i < state.count; ++i) {
      Point p = state.points.get(i);
      List<Geometry> result = state.treeIndex.query(p);
      result.removeIf(item -> !item.contains(p));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void rTreeWithIndexedPolygonBenchmark(BenchmarkState state) {
    for (int i = 0; i < state.count; ++i) {
      Point p = state.points.get(i);
      List<Geometry> result = state.indexedPolygonTree.query(p);
      result.removeIf(item -> !item.contains(p));
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(ContainsBenchmark.class.getSimpleName())
            .warmupIterations(1)
            .measurementIterations(1)
            .forks(1)
            .timeUnit(TimeUnit.MILLISECONDS)
            .build();

    new Runner(opt).run();
  }
}
