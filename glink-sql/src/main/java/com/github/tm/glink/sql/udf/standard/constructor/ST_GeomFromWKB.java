package com.github.tm.glink.sql.udf.standard.constructor;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

@SuppressWarnings("checkstyle:TypeName")
public class ST_GeomFromWKB extends ScalarFunction {

  private transient WKBReader wkbReader;

  @Override
  public void open(FunctionContext context) throws Exception {
    wkbReader = new WKBReader();
  }

  public Geometry eval(byte[] wkb) throws ParseException {
    return wkbReader.read(wkb);
  }
 }
