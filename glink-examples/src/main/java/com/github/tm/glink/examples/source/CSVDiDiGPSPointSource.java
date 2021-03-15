package com.github.tm.glink.examples.source;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.source.CSVGeoObjectSource;

/**
 * @author Yu Liebing
 */
public class CSVDiDiGPSPointSource extends CSVGeoObjectSource<Point> {

  public CSVDiDiGPSPointSource(String path) {
    super(path);
  }
  @Override
  public Point parseLine(String line) {
    String[] items = line.split(",");
    return new Point(items[1],
            Double.parseDouble(items[4]),
            Double.parseDouble(items[3]),
            Long.parseLong(items[2]) * 1000);
  }
}
