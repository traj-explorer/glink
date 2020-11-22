package com.github.tm.glink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

public class Functions {

  @SuppressWarnings("checkstyle:TypeName")
  public static class ST_Transform extends ScalarFunction {

    public Geometry eval(Geometry geom, String sourceEpsgCRSCode, String targetEpsgCRSCode) throws Exception {
      CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
      CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
      final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
      return JTS.transform(geom, transform);
    }
  }
}
