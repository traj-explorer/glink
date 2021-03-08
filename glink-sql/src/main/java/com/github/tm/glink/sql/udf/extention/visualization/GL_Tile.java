package com.github.tm.glink.sql.udf.extention.visualization;

import com.github.tm.glink.core.tile.TileGrid;
import org.apache.flink.table.functions.ScalarFunction;

@SuppressWarnings("checkstyle:TypeName")
public class GL_Tile extends ScalarFunction {

  public long eval(double lat, double lng, int level) {
    TileGrid tileGrid = new TileGrid(level);
    return tileGrid.getTile(lat, lng).toLong();
  }
}
