package com.github.tm.glink.core.tile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class TileResult<V> {
  private Tile tile;
  private List<PixelResult<V>> result;

  public TileResult() {
    this.result = new ArrayList<>();
  }

  public TileResult(Tile tile) {
    this.tile = tile;
    this.result = new ArrayList<>();
  }

  public Tile getTile() {
    return tile;
  }

  public void setTile(Tile tile) {
    this.tile = tile;
  }

  public List<PixelResult<V>> getGridResult() {
    return result;
  }

  public void setGridResult(List<PixelResult<V>> result) {
    this.result = result;
  }

  public void addPixelResult(PixelResult<V> pixelResult) {
    result.add(pixelResult);
  }

  @Override
  public String toString() {
    StringBuilder data = new StringBuilder();
    for (PixelResult<V> pixelResult : result) {
      data.append("\"").append(pixelResult.getPixel().getPixelNo()).append("\"")
              .append(":")
              .append(pixelResult.getResult())
              .append(",");
    }
    data.setLength(data.length() - 1);
    return String.format("{\"zoom_level\": %d, \"x\": %d, \"y\": %d, \"data\": {%s}}",
            tile.getLevel(), getTile().getX(), getTile().getY(), data);
  }

  public byte[] toBytes() throws IOException {
    return new AvroTileResult<V>(this).serialize();
  }

  public boolean equals(TileResult compare) {
    boolean a = compare.tile.toLong() == this.tile.toLong();
    boolean b = true;
    Iterator list1 = this.getGridResult().iterator();
    Iterator list2 = compare.getGridResult().iterator();
    while (list1.hasNext()) {
      if (!list2.hasNext()){
        b = false;
        break;
      }
      String val1 = ((PixelResult<V>)list1.next()).getResult().toString();
      String val2 = ((PixelResult<V>)list2.next()).getResult().toString();
      if (!val1.equals(val2)) {
        b = false;
        break;
      }
    }
    if (list2.hasNext()) {
      b = false;
    }
    return a && b;
  }
}
