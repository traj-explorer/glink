package com.github.tm.glink.examples.query;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.operator.DBSCAN;
import com.github.tm.glink.core.source.CSVPointSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yu Liebing
 */
public class DBSCANJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    env.setParallelism(1);

    String path = args[0];
    DataStream<Point> pointDataStream = env.addSource(new CSVPointSource(path))
            .assignTimestampsAndWatermarks(new KNNQueryJob.EventTimeAssigner(100));

    DataStream<Tuple2<Integer, Point>> clusterStream = DBSCAN.dbscan(
            pointDataStream, 1, 5000.d, 10, 0.5);
    clusterStream.print();
    clusterStream.writeAsText("D:\\code\\glink\\data\\dbscan_result.txt");

    env.execute("Pair range join");
  }
}
