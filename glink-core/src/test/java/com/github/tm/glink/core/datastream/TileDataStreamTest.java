package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.enums.TileAggregateType;
import com.github.tm.glink.core.format.Schema;
import com.github.tm.glink.core.tile.TileResult;
import junit.framework.TestCase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;
import org.locationtech.jts.geom.Point;

import java.time.Duration;

/**
 * @author Wang Haocheng
 * @date 2021/6/7 - 11:03 上午
 */
public class TileDataStreamTest{
  @Test
  public void AggregateEnumTest() throws Exception {
    // For spatial data stream source.
     final String ZOOKEEPERS = "localhost:2181";
     final String CATALOG_NAME = "Xiamen";
     final String TILE_SCHEMA_NAME = "Heatmap";
     final String POINTS_SCHEMA_NAME = "JoinedPoints";
     final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
     final int SPEED_UP = 50;
     final long WIN_LEN = 10L;
     final int PARALLELISM = 20;
     final int CARNO_FIELD_UDINDEX = 4;
     final int TIMEFIELDINDEX = 3;
     final TextFileSplitter SPLITTER = TextFileSplitter.CSV;

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    env.setParallelism(PARALLELISM);
    env.disableOperatorChaining();
    SpatialDataStream<Point> originalDataStream = new SpatialDataStream<Point>(
            env, new CSVStringSourceSimulation(FILEPATH, SPEED_UP, TIMEFIELDINDEX, SPLITTER, false),
            4, 5, TextFileSplitter.CSV, GeometryType.POINT, true,
            Schema.types(Integer.class, Double.class, Integer.class, Long.class, String.class, String.class))
            .assignTimestampsAndWatermarks((WatermarkStrategy.<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp) -> ((Tuple) event.getUserData()).getField(TIMEFIELDINDEX))));
    DataStream<TileResult> tileResultDataStream = new TileDataStream(originalDataStream,
            TileAggregateType.COUNT,
            SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)),
            1,
            13,
            13).getTileResultDataStream();
    tileResultDataStream.print();
    env.execute("test");
  }
}