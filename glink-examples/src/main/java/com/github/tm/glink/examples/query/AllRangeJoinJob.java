package com.github.tm.glink.examples.query;

import com.github.tm.glink.core.operator.AllRangeJoin;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.examples.source.CSVPointSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class AllRangeJoinJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);

    String path = args[0];
    DataStream<Point> pointDataStream = env.addSource(new CSVPointSource(path))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp)->event.getTimestamp()));

    DataStream<List<Point>> allRangeJoinStream = AllRangeJoin.allRangeJoin(
            pointDataStream, 1, 10000.d, 0.3);
    allRangeJoinStream.print();

    env.execute("All range join");
  }
}
