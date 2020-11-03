package com.github.tm.glink.format;

import com.github.tm.glink.enums.GeometryType;
import com.github.tm.glink.enums.TextFileSplitter;
import org.locationtech.jts.geom.Point;

public class PointTextFormatMap extends TextFormatMap<Point> {
  public PointTextFormatMap(
          int startOffset,
          TextFileSplitter splitter,
          boolean carryInputData) {
    super(startOffset, startOffset + 1, splitter, carryInputData, GeometryType.POINT);
  }
}
