package com.github.tm.glink.sql.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.expressions.Expression;
import org.locationtech.jts.geom.Geometry;

import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP;
import static org.apache.flink.table.api.Expressions.$;

public class Schema {
  static boolean setProcTime;

  public static Expression[] names(String... names) {
    Expression[] expressions = new Expression[names.length];
    for (int i = 0; i < names.length; ++i) {
      expressions[i] = $(names[i]);
    }
    return expressions;
  }

  /**
   *
   * @param setProcTime 如果是True，会比fields多一项，最后一项用来存proctime
   * @param names
   * @return
   */
  public static Expression[] names(boolean setProcTime, String... names) {
    Expression[] expressions;
    if(setProcTime == true) {
      expressions = new Expression[names.length + 1];
      expressions[names.length] = $("pt").proctime();
    } else {
      expressions = new Expression[names.length];
    }
    for (int i = 0; i < names.length; ++i) {
      expressions[i] = $(names[i]);
    }
    return expressions;
  }

  @Deprecated
  public static Class<?>[] types(Class<?>... types) {
    return types;
  }

  public static TypeInformation<?>[] types(TypeInformation<?>... types) {
    return types;
  }

  public static Object stringCastToPrimitive(String value, Class<?> classType) {
    if (classType.getName().equals(Integer.class.getName())) return Integer.parseInt(value);
    if (classType.getName().equals(Double.class.getName())) return Double.parseDouble(value);
    if (classType.getName().equals(Long.class.getName())) return Long.parseLong(value);
    if (classType.getName().equals(String.class.getName())) return value;
    return null;
  }

  // 如果需要设置处理时间，返回的TypeInformation[]最后一项为TIMESTAMP类型。
  public static TypeInformation<?>[] toFlinkTypes(Class<?>[] types) {
    if(setProcTime == true){
      TypeInformation<?>[] flinkTypes = new TypeInformation<?>[types.length+1];
      flinkTypes[types.length] = TIMESTAMP;
    }
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
