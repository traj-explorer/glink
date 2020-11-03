package com.github.tm.glink.enums;

import java.io.Serializable;

/**
 * @author Yu Liebing
 */
public enum GeometryType implements Serializable {

  POINT,
  POLYGON,
  LINESTRING,
  MULTIPOINT,
  MULTIPOLYGON,
  MULTILINESTRING,
  GEOMETRYCOLLECTION,
  CIRCLE,
  RECTANGLE;

  /**
   * Gets the GeometryType.
   *
   * @param str the str
   * @return the GeometryType
   */
  public static GeometryType getGeometryType(String str) {
    for (GeometryType me : GeometryType.values()) {
      if (me.name().equalsIgnoreCase(str)) {
        return me;
      }
    }
    throw new IllegalArgumentException("[" + GeometryType.class + "] Unsupported geometry type:" + str);
  }
}
