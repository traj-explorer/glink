package com.github.tm.glink.examples.query;

import com.github.tm.glink.examples.source.CSVDiDiGPSPointSource;
import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.operator.AllRangeJoin;
import com.github.tm.glink.source.CSVPointSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class AllRangeJoinJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);

    String path = args[0];
    DataStream<Point> pointDataStream = env.addSource(new CSVDiDiGPSPointSource(path))
            .assignTimestampsAndWatermarks(new KNNQueryJob.EventTimeAssigner(100));

    DataStream<List<Point>> allRangeJoinStream = AllRangeJoin.allRangeJoin(
            pointDataStream, 1, 300.d, 0.01);
    allRangeJoinStream.print();

    env.execute("All range join");
  }
}
