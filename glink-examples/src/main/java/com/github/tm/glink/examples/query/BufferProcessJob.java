package com.github.tm.glink.examples.query;

import com.github.tm.glink.examples.source.CSVDiDiGPSPointSource;
import com.github.tm.glink.fearures.BoundingBox;
import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.operator.BufferProcess;
import com.github.tm.glink.source.CSVPointSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class BufferProcessJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
//    env.getConfig().setAutoWatermarkInterval(1000L);

    String path = KNNQueryJob.class.getResource("/gps_20161101_0710").getPath();
    DataStream<Point> pointDataStream = env.addSource(new CSVDiDiGPSPointSource(path))
            .assignTimestampsAndWatermarks(new KNNQueryJob.EventTimeAssigner(500));

    DataStream<List<Point>> bufferResult = BufferProcess.bufferProcess(
            pointDataStream,
            new BoundingBox(30.652828, 30.727818, 104.042102, 104.129591),
            4, 4,
            500,
            1,
            false,
            0);
    bufferResult.print();

    env.execute();
  }
}
