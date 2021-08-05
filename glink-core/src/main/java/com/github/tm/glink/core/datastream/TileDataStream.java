package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.enums.TileAggregateType;
import com.github.tm.glink.core.tile.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Point;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


/**
 * @param <T> The ACC type of {@link AggregateFunction}.
 */
public class TileDataStream<T> {

  private DataStream<TileResult> tileResultDataStream;

  /**
   *
   * @param aggFieldIndex The index of the field in the user data to be aggregated. If the aggregateType is COUNT, this parameter is useless.
   */
  public TileDataStream(
          SpatialDataStream<Point> pointDataStream,
          TileAggregateType aggregateType,
          WindowAssigner windowAssigner,
          int aggFieldIndex,
          int hLevel,
          int lLevel) {
    tileResultDataStream = pointDataStream.getDataStream()
            .flatMap(new PixelGenerateFlatMap(hLevel, lLevel))
            .keyBy(new DefaultKeyByTile())
            .window(windowAssigner)
            .aggregate(TileAggregateType.getAggregateFunction(aggregateType, aggFieldIndex), new AddWindowTimeInfo());
  }

  public TileDataStream(
          SpatialDataStream<Point> pointDataStream,
          AggregateFunction<Tuple2<Tuple2<PixelResult, Point>, String>, Map<Pixel, T>, TileResult> aggregateFunction,
          WindowAssigner windowAssigner,
          int hLevel,
          int lLevel,
          Boolean addSalt,
          AggregateFunction<TileResult, TileResult, TileResult> finalTileAggregate) {
    if (addSalt) {
      tileResultDataStream = pointDataStream.getDataStream()
              .flatMap(new PixelGenerateFlatMap(hLevel, lLevel))
              .returns(TypeInformation.of(new TypeHint<Tuple2<PixelResult<Integer>, Point>>() { }))
              // 预聚合
              .map(r -> new Tuple2<Tuple2<PixelResult<Integer>, Point>, String>(r, r.f0.getPixel().getTile().toString() + ThreadLocalRandom.current().nextLong(1, 20)))
              .returns(TypeInformation.of(new TypeHint<Tuple2<Tuple2<PixelResult<Integer>, Point>, String>>() { }))
              .keyBy(r -> r.f1)
              .window(windowAssigner)
              .aggregate(aggregateFunction)
              // final aggregate
              .keyBy(new KeyByTileWithOutSalt())
              .window(windowAssigner)
              .aggregate(finalTileAggregate, new AddWindowTimeInfo());
    } else {
      tileResultDataStream = pointDataStream.getDataStream()
              .flatMap(new PixelGenerateFlatMap(hLevel, lLevel))
              .keyBy(new DefaultKeyByTile())
              // 预聚合
              .window(windowAssigner)
              .aggregate(aggregateFunction, new AddWindowTimeInfo());
    }
  }

  public DataStream<TileResult> getTileResultDataStream() {
    return tileResultDataStream;
  }

  private class PixelGenerateFlatMap extends RichFlatMapFunction<Point, Tuple2<PixelResult<Integer>, Point>> {
    private static final long serialVersionUID = 5235060756502253407L;
    private transient TileGrid[] tileGrids;
    int hLevel, lLevel, levelNum;

    PixelGenerateFlatMap(int hLevel, int lLevel) {
      this.hLevel = hLevel;
      this.lLevel = lLevel;
      levelNum = hLevel - lLevel + 1;
    }

    @Override
    public void open(Configuration conf) {
      int length = hLevel - lLevel + 1;
      tileGrids = new TileGrid[length];
      int i = length;
      int j = hLevel;
      while (0 < i) {
        tileGrids[i - 1] = new TileGrid(j);
        i--;
        j--;
      }
    }

    @Override
    public void flatMap(Point value, Collector<Tuple2<PixelResult<Integer>, Point>> out) throws Exception {
      int i = levelNum;
      while (0 < i) {
        out.collect(new Tuple2<>(new PixelResult<>(tileGrids[i - 1].getPixel(value.getY(), value.getX()), 1), value));
        i = i - 1;
      }
    }
  }

  private class DefaultKeyByTile implements KeySelector<Tuple2<PixelResult<Integer>, Point>, Tile> {
    private static final long serialVersionUID = 406340347008662020L;
    @Override
    public Tile getKey(Tuple2<PixelResult<Integer>, Point> value) {
      return value.f0.getPixel().getTile();
    }
  }

  private class KeyByTileWithOutSalt implements KeySelector<TileResult, Tile> {
    private static final long serialVersionUID = 2776649793200324329L;
    @Override
    public Tile getKey(TileResult value) {
      return value.getTile();
    }
  }

  private class KeyByTileWithSalt implements KeySelector<Tuple2<Tuple2<PixelResult, Point>, String>, String> {
    private static final long serialVersionUID = 2776649793200324329L;
    @Override
    public String getKey(Tuple2<Tuple2<PixelResult, Point>, String> tuple2StringTuple2) throws Exception {
      return tuple2StringTuple2.f1;
    }
  }

  private static class AddWindowTimeInfo extends ProcessWindowFunction<TileResult, TileResult, Tile, TimeWindow> {

    private static final long serialVersionUID = -1308201162807418668L;

    @Override
    public void process(Tile tile, Context context, Iterable<TileResult> elements, Collector<TileResult> out) throws Exception {
      TileResult tileResult = elements.iterator().next();
      tileResult.setTimeStart(new Timestamp(context.window().getStart()));
      tileResult.setTimeEnd(new Timestamp(context.window().getEnd()));
      out.collect(tileResult);
    }
  }
}
