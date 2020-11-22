package com.github.tm.glink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

public class GlinkFunctions {

  @SuppressWarnings("checkstyle:TypeName")
  public static class GL_PointToCSV extends ScalarFunction {

    public static String eval(Geometry geom) {
      Point point = (Point) geom;
      System.out.println(point.getX() + "," + point.getY());
      return point.getX() + "," + point.getY();
    }
  }
}
