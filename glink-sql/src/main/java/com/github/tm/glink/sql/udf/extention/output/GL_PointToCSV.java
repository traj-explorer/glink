package com.github.tm.glink.sql.udf.extention.output;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

@SuppressWarnings("checkstyle:TypeName")
public class GL_PointToCSV extends ScalarFunction {

  public static String eval(Geometry geom) {
    Point point = (Point) geom;
    return point.getX() + "," + point.getY();
  }
}
