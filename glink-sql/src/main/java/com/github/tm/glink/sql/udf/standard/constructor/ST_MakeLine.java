package com.github.tm.glink.sql.udf.standard.constructor;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import java.util.List;

@SuppressWarnings("checkstyle:TypeName")
public class ST_MakeLine extends ScalarFunction {

  private transient GeometryFactory geometryFactory;

  @Override
  public void open(FunctionContext context) throws Exception {
    geometryFactory = new GeometryFactory();
  }

  public LineString eval(List<Point> points) {
    return geometryFactory.createLineString(points.stream().map(Point::getCoordinate).toArray(Coordinate[]::new));
  }
}
