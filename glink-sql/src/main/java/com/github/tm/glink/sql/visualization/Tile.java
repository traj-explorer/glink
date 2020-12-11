package com.github.tm.glink.sql.visualization;

import com.github.tm.glink.core.tile.TileGrid;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author Yu Liebing
 */
public class Tile {

  public class ST_Tile extends ScalarFunction {

    public long eval(double lat, double lng, int level) {
      return TileGrid.getTile(lat, lng, level).toLong();
    }
  }

  public class ST_Heatmap extends ScalarFunction {
  }
  
}
