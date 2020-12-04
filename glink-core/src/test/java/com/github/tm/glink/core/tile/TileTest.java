package com.github.tm.glink.core.tile;

import org.junit.Test;

import static org.junit.Assert.*;

public class TileTest {

  @Test
  public void fromLong() {
    Tile tile = new Tile(12, 789, 8910);
    Tile newTile = tile.fromLong(tile.toLong());
    System.out.println(tile);
    System.out.println(newTile);
  }
}