package com.github.tm.glink.benchmark.tdrive;

import com.github.tm.glink.core.util.GeoUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class GridVsDistanceBenchmark {

  @State(Scope.Thread)
  public static class BenchmarkState {
    private int count = 1000000;
    private Set<Long> set = new HashSet<>(count);
    private List<Point> points = new ArrayList<>(10000);

    @Setup
    public void setUp() {
      Random random = new Random();
      GeometryFactory factory = new GeometryFactory();
      for (int i = 0; i < 1000001; ++i) {
        Point p = factory.createPoint(new Coordinate(random.nextDouble() * 180, random.nextDouble() * 90));
        points.add(p);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void gridBenchmark(BenchmarkState state) {
    List<Long> result = new ArrayList<>();
    for (long i = 0; i < 1000000; ++i) {
      if (state.set.contains(i)) {
        result.add(i);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void distanceBenchmark(BenchmarkState state) {
    for (int i = 1, len = state.points.size(); i < len; ++i) {
      double dis = GeoUtils.calcDistance(state.points.get(0), state.points.get(i));
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(GridVsDistanceBenchmark.class.getSimpleName())
            .warmupIterations(1)
            .measurementIterations(1)
            .forks(1)
            .timeUnit(TimeUnit.MILLISECONDS)
            .build();

    new Runner(opt).run();
  }
}
