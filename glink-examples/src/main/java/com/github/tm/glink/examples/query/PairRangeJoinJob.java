package com.github.tm.glink.examples.query;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.operator.PairRangeJoin;
import com.github.tm.glink.source.CSVPointSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
            .assignTimestampsAndWatermarks(new KNNQueryJob.EventTimeAssigner(100));

    DataStream<Tuple2<Point, Point>> pairRangeJoinStream = PairRangeJoin.pairRangeJoin(
            pointDataStream, 1, 20000.d, 0.4);
    pairRangeJoinStream.print();

    env.execute("Pair range join");
  }
}