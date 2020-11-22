package com.github.tm.glink.datastream;

import com.github.tm.glink.grid.TileResult;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author Yu Liebing
 */
public class GridResultDataStream<V> {

  private DataStream<TileResult<V>> gridResultDataStream;

  protected GridResultDataStream(DataStream<TileResult<V>> gridResultDataStream) {
    this.gridResultDataStream = gridResultDataStream;
  }

  public void print() {
    gridResultDataStream.print();
  }
}
