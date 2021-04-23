package com.github.tm.glink.benchmark.tdrive;

import org.apache.flink.api.java.tuple.Tuple;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public class Utils {

  public static boolean isGeometryListAttrEqual(List<Geometry> l1, List<Geometry> l2) {
    int len1 = l1.size(), len2 = l2.size();
    if (len1 != len2) return false;
    for (int i = 0; i < len1; ++i) {
      Tuple a1 = (Tuple) l1.get(i).getUserData();
      Tuple a2 = (Tuple) l2.get(i).getUserData();
      if (!a1.equals(a2)) return false;
    }
    return true;
  }
}
