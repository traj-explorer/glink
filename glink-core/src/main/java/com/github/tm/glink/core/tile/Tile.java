package com.github.tm.glink.core.tile;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Yu Liebing
 */
public class Tile implements Serializable {
  private int level;
  private int x;
  private int y;

  public Tile(int level, int x, int y) {
    this.level = level;
    this.x = x;
    this.y = y;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public int getX() {
    return x;
  }

  public void setX(int x) {
    this.x = x;
  }

  public int getY() {
    return y;
  }

  public void setY(int y) {
    this.y = y;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Tile tile = (Tile) o;
    return level == tile.level &&
            x == tile.x &&
            y == tile.y;
  }

  @Override
  public int hashCode() {
    return Objects.hash(level, x, y);
  }

  @Override
  public String toString() {
    return String.format("Tile{level=%d, x=%d, y=%d}", level, x, y);
  }

  public long toLong() {
    long res = 0L;
    res |= ((long) level) << 46;
    res |= ((long) x) << 23;
    res |= y;
    return res;
  }

  public Tile fromLong(long tileId) {
    int level  = (int) (tileId >> 46);
    int x = (int) ((tileId & (0x007f_ffffL << 23)) >> 23);
    int y = (int) (tileId & (0x007f_ffffL));
    return new Tile(level, x, y);
  }
}
