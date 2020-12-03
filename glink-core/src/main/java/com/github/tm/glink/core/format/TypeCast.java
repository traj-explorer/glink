package com.github.tm.glink.core.format;

public class TypeCast {

  public static Object stringCastToPrimitive(String value, Class<?> classType) {
    if (classType.getName().equals(Integer.class.getName())) {
      return Integer.parseInt(value);
    } else if (classType.getName().equals(Double.class.getName())) {
      return Double.parseDouble(value);
    }
    return null;
  }

  public static void main(String[] args) {
    String value = "12";
    Class<Integer> integerClass = Integer.class;
    int v = integerClass.cast(stringCastToPrimitive(value, integerClass));
    System.out.println(v);
  }
}
