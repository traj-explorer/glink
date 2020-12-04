package com.github.tm.glink.sql.visualization;

import com.github.tm.glink.core.tile.TileGrid;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author Yu Liebing
 */
public class Tile {

  public class ST_Tile extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
      super.open(context);
    }

    public long eval(double lat, double lng, int level) {
      TileGrid tileGrid = new TileGrid(level);

      return 0;
    }
  }
  
}
