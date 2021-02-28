package com.github.tm.glink.sql.udf.standard.constructor;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

@SuppressWarnings("checkstyle:TypeName")
public class ST_Polygon extends ScalarFunction {

  private transient GeometryFactory geometryFactory;

  @Override
  public void open(FunctionContext context) throws Exception {
    geometryFactory = new GeometryFactory();
  }

  public Polygon eval(LineString shell) {
    return geometryFactory.createPolygon(shell.getCoordinates());
  }
}
