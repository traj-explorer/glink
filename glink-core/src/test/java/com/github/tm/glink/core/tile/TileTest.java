package com.github.tm.glink.core.tile;

import org.junit.Test;

public class TileTest {

  @Test
  public void fromLong() {
    Tile tile = new Tile(12, 5, 4);
    Tile newTile = tile.fromLong(tile.toLong());
    System.out.println(tile);
    System.out.println(newTile);
  }
}