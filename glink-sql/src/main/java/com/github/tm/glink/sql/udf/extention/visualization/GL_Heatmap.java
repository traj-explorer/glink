package com.github.tm.glink.sql.udf.extention.visualization;

import com.github.tm.glink.core.tile.Pixel;
import com.github.tm.glink.core.tile.TileGrid;
import com.github.tm.glink.core.tile.TileResult;
import org.apache.flink.table.functions.AggregateFunction;

@SuppressWarnings("checkstyle:TypeName")
public class GL_Heatmap extends AggregateFunction<TileResult<Integer>, TileResult<Integer>> {

  @Override
  public TileResult<Integer> getValue(TileResult<Integer> integerTileResult) {
    return null;
  }

  @Override
  public TileResult<Integer> createAccumulator() {
    return new TileResult<>();
  }

  public void accumulate(Integer acc, double lat, double lng, int level) {
    TileGrid tileGrid = new TileGrid(level);
    Pixel pixel = tileGrid.getPixel(lat, lng);
  }
}
