package com.github.tm.glink.sql.udf.standard.relationship;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

@SuppressWarnings("checkstyle:TypeName")
public class ST_Contains extends ScalarFunction {

  public boolean eval(Geometry geom1, Geometry geom2) {
    return geom1.contains(geom2);
  }
}
