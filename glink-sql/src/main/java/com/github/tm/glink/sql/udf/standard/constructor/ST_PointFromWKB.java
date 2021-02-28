package com.github.tm.glink.sql.udf.standard.constructor;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

@SuppressWarnings("checkstyle:TypeName")
public class ST_PointFromWKB extends ScalarFunction {

  private transient WKBReader wkbReader;

  @Override
  public void open(FunctionContext context) throws Exception {
    wkbReader = new WKBReader();
  }

  public Point eval(byte[] wkb) throws ParseException {
    return (Point) wkbReader.read(wkb);
  }
}
