package com.github.tm.glink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

public class Predicates {

  @SuppressWarnings("checkstyle:TypeName")
  public static class ST_Contains extends ScalarFunction {
    public boolean eval(Geometry geom1, Geometry geom2) {
      return geom1.contains(geom2);
    }
  }
}
