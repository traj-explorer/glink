package com.github.tm.glink.sql.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class GeometryConstructor {

  @SuppressWarnings("checkstyle:TypeName")
  public static class ST_Point extends ScalarFunction {

    private transient GeometryFactory factory;

    @Override
    public void open(FunctionContext context) throws Exception {
      factory = new GeometryFactory();
    }

    public Point eval(double x, double y, String... uuids) {
      Point point = factory.createPoint(new Coordinate(x, y));
      String userData = String.join("\t", uuids);
      if (!userData.equals(""))
        point.setUserData(userData);
      return point;
    }
  }
}
