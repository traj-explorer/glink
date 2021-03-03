package com.github.tm.glink.examples.query;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.operator.PairRangeJoin;
import com.github.tm.glink.examples.source.CSVPointSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author Yu Liebing
 */
public class PairRangeJoinJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    env.setParallelism(1);

    String path = args[0];
    DataStream<Point> pointDataStream = env.addSource(new CSVPointSource(path))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp)->event.getTimestamp()));

    DataStream<Tuple2<Point, Point>> pairRangeJoinStream = PairRangeJoin.pairRangeJoin(
            pointDataStream, 1, 20000.d, 0.4);
    pairRangeJoinStream.print();

    env.execute("Pair range join");
  }
}
