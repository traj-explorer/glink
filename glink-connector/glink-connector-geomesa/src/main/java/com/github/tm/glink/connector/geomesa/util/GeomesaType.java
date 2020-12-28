package com.github.tm.glink.connector.geomesa.util;

import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/**
 * Enumeration of types supported by geomesa.
 * More details refer to https://www.geomesa.org/documentation/stable/user/datastores/attributes.html
 *
 * @author Yu Liebing
 * */
public enum  GeomesaType {
  // basic types
  STRING("String"),
  INTEGER("Integer"),
  DOUBLE("Double"),
  LONG("Long"),
  FLOAT("Float"),
  BOOLEAN("Boolean"),
  UUID("UUID"),
  DATE("Date"),
  TIMESTAMP("Timestamp"),
  // Geometry types
  POINT("Point"),
  LINE_STRING("LineString"),
  POLYGON("Polygon"),
  MULTI_POINT("MultiPoint"),
  MULTI_LINE_STRING("MultiLineString"),
  MULTI_POLYGON("MultiPolygon"),
  GEOMETRY_COLLECTION("GeometryCollection"),
  GEOMETRY("Geometry"),
  BYTES("Bytes");
  // complex types

  private static final int MIN_TIMESTAMP_PRECISION = 0;
  private static final int MAX_TIMESTAMP_PRECISION = 3;
  private static final int MIN_TIME_PRECISION = 0;
  private static final int MAX_TIME_PRECISION = 3;

  private final String name;

  GeomesaType(String name) {
    this.name = name;
  }

  public static GeomesaType getGeomesaType(String name) {
    GeomesaType[] types = GeomesaType.values();
    for (GeomesaType type : types) {
      if (type.name.equalsIgnoreCase(name))
        return type;
    }
    throw new IllegalArgumentException("[" + GeomesaType.class + "] Unsupported geometry type:" + name);
  }

  public static GeomesaType mapLogicalTypeToGeomesaType(LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return STRING;
      case BOOLEAN:
        return BOOLEAN;
      case BINARY:
      case VARBINARY:
        return BYTES;
      case DECIMAL:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case DATE:
      case INTERVAL_YEAR_MONTH:
        return INTEGER;
      case TIME_WITHOUT_TIME_ZONE:
        final int timePrecision = getPrecision(logicalType);
        if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIME type is out of the range [%s, %s] supported by " +
                          "HBase connector", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
        }
        return TIMESTAMP;
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return LONG;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final int timestampPrecision = getPrecision(logicalType);
        if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by " +
                          "HBase connector", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
        }
        return TIMESTAMP;
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }
}
