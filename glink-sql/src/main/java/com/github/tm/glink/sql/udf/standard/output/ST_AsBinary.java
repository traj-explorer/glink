package com.github.tm.glink.sql.udf.standard.output;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

@SuppressWarnings("checkstyle:TypeName")
public class ST_AsBinary extends ScalarFunction {

  private transient WKBWriter wkbWriter;

  @Override
  public void open(FunctionContext context) throws Exception {
    wkbWriter = new WKBWriter();
  }

  public byte[] eval(Geometry geom) {
    return wkbWriter.write(geom);
  }
}
