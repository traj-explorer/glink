package com.github.tm.glink.core.types;

public class BasicTypeInfo<T> extends TypeInfo<T> {

  public static BasicTypeInfo<Integer> INTEGER = new BasicTypeInfo<>(Integer.class);
  public static BasicTypeInfo<Long> LONG = new BasicTypeInfo<>(Long.class);
  public static BasicTypeInfo<Double> DOUBLE = new BasicTypeInfo<>(Double.class);

  private Class<T> clazz;

  protected BasicTypeInfo(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T from(String value) {
    if (clazz.getName().equals(Integer.class.getName())) return (T) new Integer(Integer.parseInt(value));
    if (clazz.getName().equals(Long.class.getName())) return (T) new Long(Long.parseLong(value));
    if (clazz.getName().equals(Double.class.getName())) return (T) new Double(Double.parseDouble(value));
    return null;
  }
}
