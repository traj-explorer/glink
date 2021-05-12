package com.github.tm.glink.examples.source;

import com.github.tm.glink.core.source.CSVGeoObjectSource;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

/**
 * Data example:
 * 6[OPERATING_STATUS],
 * 15.00[SPEED],
 * 33, (DRIVING_DIRECTION)
 * 1559805482000, (TIMESTAMP)
 * 118.148850, (LONGITUDE)
 * 24.532240, (LATITUDE)
 * 6577840df8d4caf980c065aeb70f165d(CAR ID)
 * @author Wang Haocheng
 *
 */
@Deprecated
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
//      int pid = Integer.parseInt(items[7]);
      double lat = Double.parseDouble(items[5]);
      double lng = Double.parseDouble(items[4]);
      long timestamp = Long.parseLong(items[3]);
      Properties properties = new Properties();
      properties.put("speed",Double.parseDouble(items[1]));
      properties.put("azimuth",Double.parseDouble(items[2]));
      properties.put("OperatingStatus",Integer.parseInt(items[0]));
      TrajectoryPoint r  = new TrajectoryPoint(carNo, 0, lat, lng, timestamp);
      r.setAttributes(properties);
      return r;
    } catch (Exception e) {
      System.out.println(e);
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    Long start = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8));
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    String path = CSVXiamenTrajectorySource.class.getClassLoader().getResource("XiamenTrajDataCleaned.csv").getPath();
    DataStream<TrajectoryPoint> pointDataStream = env.addSource(new CSVXiamenTrajectorySource(path,100));
    pointDataStream.print();
    env.execute();
    Long end = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8));
    System.out.println("Time used: " + (end - start) + " seconds.");
  }
}