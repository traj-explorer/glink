package com.github.tm.glink.sql.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.expressions.Expression;
import org.locationtech.jts.geom.Geometry;

import static org.apache.flink.table.api.Expressions.$;

public class Schema {

  public static Expression[] names(String... names) {
    Expression[] expressions = new Expression[names.length];
    for (int i = 0; i < names.length; ++i) {
      expressions[i] = $(names[i]);
    }
    return expressions;
  }

  public static Class<?>[] types(Class<?>... types) {
    return types;
  }

  public static Object stringCastToPrimitive(String value, Class<?> classType) {
    if (classType.getName().equals(Integer.class.getName())) return Integer.parseInt(value);
    if (classType.getName().equals(Double.class.getName())) return Double.parseDouble(value);
    if (classType.getName().equals(Long.class.getName())) return Long.parseLong(value);
    if (classType.getName().equals(String.class.getName())) return value;
    return null;
  }

  public static TypeInformation<?>[] toFlinkTypes(Class<?>[] types) {
    TypeInformation<?>[] flinkTypes = new TypeInformation<?>[types.length];
    for (int i =  0; i < types.length; ++i) {
      flinkTypes[i] = getFlinkTypeInformation(types[i]);
    }
    return flinkTypes;
  }

  private static TypeInformation<?> getFlinkTypeInformation(Class<?> type) {
    if (type.getName().equals(Void.class.getName())) return Types.VOID;
    if (type.getName().equals(String.class.getName())) return Types.STRING;
    if (type.getName().equals(Byte.class.getName())) return Types.BYTE;
    if (type.getName().equals(Boolean.class.getName())) return Types.BOOLEAN;
    if (type.getName().equals(Short.class.getName())) return Types.SHORT;
    if (type.getName().equals(Integer.class.getName())) return Types.INT;
    if (type.getName().equals(Long.class.getName())) return Types.LONG;
    if (type.getName().equals(Float.class.getName())) return Types.FLOAT;
    if (type.getName().equals(Double.class.getName())) return Types.DOUBLE;
    if (type.getSuperclass().getName().equals(Geometry.class.getName())) return TypeInformation.of(type);
    throw new IllegalArgumentException("Only supported for primitive types");
  }
}
