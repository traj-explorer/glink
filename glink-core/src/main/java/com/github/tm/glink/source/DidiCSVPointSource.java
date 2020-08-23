package com.github.tm.glink.source;

import org.apache.flink.configuration.Configuration;

import com.github.tm.glink.features.Point;

/**
 * @author Wang Haocheng
 */
public class DidiCSVPointSource extends CSVGeoObjectSource<Point> {


  public DidiCSVPointSource(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public Point parseLine(String line) {
    String[] items = line.split(",");
    long timestamp = Long.parseLong(items[2]);
    return new Point(
        items[0],
        Float.parseFloat(items[4]),
        Float.parseFloat(items[3]),
        timestamp);
  }
}
