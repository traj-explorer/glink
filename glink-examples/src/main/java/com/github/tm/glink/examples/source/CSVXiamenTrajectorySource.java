package com.github.tm.glink.examples.source;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.core.source.CSVGeoObjectSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Data example:
 * 6[OPERATING_STATUS],
 * 15.00[SPEED],
 * 33, (DRIVING_DIRECTION)
 * 2019/6/6, (DATETIME)
 * 118.148850, (LONGITUDE)
 * 24.532240, (LATITUDE)
 * 6577840df8d4caf980c065aeb70f165d(CAR ID)
 * @author Wang Haocheng
 *
 */
public class CSVXiamenTrajectorySource extends CSVGeoObjectSource<TrajectoryPoint> {
  public CSVXiamenTrajectorySource(String path) {
    super(path);
  }

  public CSVXiamenTrajectorySource(String path, int i) {
    super(path, i);
  }

  @Override
  public TrajectoryPoint parseLine(String line) {
    try {
      String[] items = line.split(",");
      String carNo = items[6];
      double lat = Double.parseDouble(items[5]);
      double lng = Double.parseDouble(items[4]);
      long timestamp = Long.parseLong(items[3]);
      return new TrajectoryPoint(carNo, 0, lat, lng, timestamp);
    } catch (Exception e) {
      System.out.println(e);
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    Long start = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8));
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    // 设置hdfs环境与文件路径
    String path = CSVXiamenTrajectorySource.class.getClassLoader().getResource("XiamenTrajDataCleaned.csv").getPath();
    // 生成流，执行热力图计算
    DataStream<TrajectoryPoint> pointDataStream = env.addSource(new CSVXiamenTrajectorySource(path,10));
    pointDataStream.print();
    env.execute();
    Long end = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8));
    System.out.println("Time used: " + (end - start));
  }
}