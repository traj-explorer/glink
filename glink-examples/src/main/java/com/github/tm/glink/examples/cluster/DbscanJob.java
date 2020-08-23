package com.github.tm.glink.examples.cluster;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.github.tm.glink.examples.query.KNNQueryJob;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.operator.DBSCAN;
import com.github.tm.glink.source.DidiCSVPointSource;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class DbscanJob {
  private static int clusterId = 0;
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    String path = args[0];
    // 使用gps_20161101_0710作为实验数据
    DataStream<Point> pointDataStream = env.addSource(new DidiCSVPointSource(path))
        .assignTimestampsAndWatermarks(new KNNQueryJob.EventTimeAssigner(100));
    int windowSize = 1;
    double distance = 30.d;
    double gridWidth = 0.01;
    int minPts = 4;
    DBSCAN.processDbscan(pointDataStream, windowSize, distance, gridWidth, minPts)
        .map(new MapFunction<List<Point>, Boolean>() {
          @Override
          public Boolean map(List<Point> list) throws Exception {
            File file = new File("/Users/haocheng/Desktop/glink/glink-examples/src/main/resources/cluster_result.txt");
            FileWriter fw = new FileWriter(file, true);
            for (Point point: list) {
              fw.write(point.getLat() + "," + point.getLng() + "," + clusterId);
              fw.write("\n");
            }
            clusterId++;
            fw.close();
            return true;
          }
        });
    env.execute("Dbscan job.");
  }
}
