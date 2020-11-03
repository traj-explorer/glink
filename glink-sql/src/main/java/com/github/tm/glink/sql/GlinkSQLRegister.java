package com.github.tm.glink.sql;

import com.github.tm.glink.sql.udf.GeometryConstructor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Yu Liebing
 */
public class GlinkSQLRegister {

  public static void registerUDF(StreamTableEnvironment tEnv) {
    tEnv.createFunction("ST_Point", GeometryConstructor.ST_Point.class);
  }
}
