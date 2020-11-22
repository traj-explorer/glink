package com.github.tm.glink.examples.visualization;

import com.github.tm.glink.datastream.GridResultDataStream;
import com.github.tm.glink.datastream.SpatialDataStream;
import com.github.tm.glink.enums.TextFileSplitter;
import com.github.tm.glink.grid.TileGrid;
import com.github.tm.glink.sql.util.Schema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class GridExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(1L);

    SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
            env, 0, TextFileSplitter.COMMA,
            Schema.types(Integer.class, String.class, Long.class),
            "22.3,33.4,1,hangzhou,1606014806522", "22.4,33.6,2,wuhan,1606014808522");

    GridResultDataStream<Integer> gridDataStream = pointDataStream
            .grid(TileGrid.MAX_LEVEL).gridSum();

    gridDataStream.print();

    env.execute();
  }
}
