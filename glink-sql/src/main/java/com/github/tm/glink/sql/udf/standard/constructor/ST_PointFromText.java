package com.github.tm.glink.sql.udf.standard.constructor;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

@SuppressWarnings("checkstyle:TypeName")
public class ST_PointFromText extends ScalarFunction {

  private transient WKTReader wktReader;

  @Override
  public void open(FunctionContext context) throws Exception {
    wktReader = new WKTReader();
  }

  public Point eval(String wkt) throws ParseException {
    return (Point) wktReader.read(wkt);
  }
}
