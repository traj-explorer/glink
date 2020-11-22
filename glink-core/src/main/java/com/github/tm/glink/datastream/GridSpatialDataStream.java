package com.github.tm.glink.datastream;

import com.github.tm.glink.grid.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public class GridSpatialDataStream<T extends Geometry> extends SpatialDataStream<T> {

  private DataStream<T> gridDataStream;

  public GridSpatialDataStream(SpatialDataStream<T> spatialDataStream) {
    this(spatialDataStream, TileGrid.MAX_LEVEL);
  }

  public GridSpatialDataStream(SpatialDataStream<T> spatialDataStream, int maxLevel) {
    gridDataStream = spatialDataStream.getDataStream().map(new PixelMap<>(maxLevel));
  }

  public GridResultDataStream<Integer> gridSum() {
    gridDataStream = gridDataStream.assignTimestampsAndWatermarks(new SensorTimeAssigner<>());

    DataStream<PixelResult<Integer>> pixelResultStream = gridDataStream
            .keyBy(r -> {
      Tuple gridWithAttributes = (Tuple) r.getUserData();
      return gridWithAttributes.<Pixel>getField(0);
    }).window(TumblingEventTimeWindows.of(Time.seconds(1))).apply(new WindowFunction<T, PixelResult<Integer>, Pixel, TimeWindow>() {
              @Override
              public void apply(Pixel pixel, TimeWindow timeWindow, Iterable<T> iterable, Collector<PixelResult<Integer>> collector) throws Exception {
                int count = 0;
                for (T geom : iterable) {
                  ++count;
                }
                collector.collect(new PixelResult<>(pixel, count));
              }
            });

    DataStream<TileResult<Integer>> tileResultDataStream = pixelResultStream.keyBy(r -> r.getPixel().getTile()).window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .apply(new WindowFunction<PixelResult<Integer>, TileResult<Integer>, Tile, TimeWindow>() {
              @Override
              public void apply(Tile tile, TimeWindow timeWindow, Iterable<PixelResult<Integer>> iterable, Collector<TileResult<Integer>> collector) throws Exception {
                TileResult<Integer> tileResult = new TileResult<>(tile);
                for (PixelResult<Integer> pixelResult : iterable) {
                  tileResult.addPixelResult(pixelResult);
                }
                collector.collect(tileResult);
              }
            });

    return new GridResultDataStream<>(tileResultDataStream);
  }

  public GridSpatialDataStream<T> gridAggregate() {
    return null;
  }

  @Override
  public DataStream<T> getDataStream() {
    return gridDataStream;
  }

  @Override
  public void print() {
    gridDataStream
            .map(r -> r + " " + r.getUserData())
            .print();
  }

  private static class PixelMap<T extends Geometry> extends RichMapFunction<T, T> {

    private final int level;
    private TileGrid tileGrid;

    public PixelMap(int level) {
      this.level = level;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      tileGrid = new TileGrid(level);
    }

    @Override
    public T map(T geom) throws Exception {
      Tuple attributes = (Tuple) geom.getUserData();
      Pixel pixel = tileGrid.getPixel(geom);
      Tuple2<Pixel, Tuple> gridWithAttributes = new Tuple2<>(pixel, attributes);
      geom.setUserData(gridWithAttributes);
      return geom;
    }
  }
}
