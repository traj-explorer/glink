package com.github.tm.glink.examples.source;

import com.github.tm.glink.feature.Point;
import com.github.tm.glink.source.CSVGeoObjectSource;

/**
 * @author Yu Liebing
 */
public class CSVDiDiGPSPointSource extends CSVGeoObjectSource<Point> {

  public CSVDiDiGPSPointSource(String filePath) {
    this.filePath = filePath;
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
