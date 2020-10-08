package com.github.tm.glink.examples.source;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.source.CSVGeoObjectSource;

/**
 * @author Yu Liebing
 */
public class CSVXiamenTrajectorySource extends CSVGeoObjectSource<TrajectoryPoint> {

  public CSVXiamenTrajectorySource(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public TrajectoryPoint parseLine(String line) {
    String[] items = line.split(",");
    int pid = Integer.parseInt(items[0]);
    String id = items[1];
    double lat = Double.parseDouble(items[2]);
    double lng = Double.parseDouble(items[3]);
    long timestamp = Long.parseLong(items[6]) * 1000;
    return new TrajectoryPoint(id, pid, lat, lng, timestamp);
  }
}
