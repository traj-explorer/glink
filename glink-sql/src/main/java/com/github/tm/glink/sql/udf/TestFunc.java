package com.github.tm.glink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class TestFunc extends ScalarFunction {

  public boolean eval(String v1, String v2) {
    return v1.equals(v2);
  }
}
