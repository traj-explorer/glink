package com.github.tm.glink.sql;

import com.github.tm.glink.sql.udf.Functions;
import com.github.tm.glink.sql.udf.GeometryConstructor;
import com.github.tm.glink.sql.udf.GlinkFunctions;
import com.github.tm.glink.sql.udf.Predicates;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Yu Liebing
 */
public class GlinkSQLRegister {

  public static void registerUDF(StreamTableEnvironment tEnv) {
    // geometry constructor
    tEnv.registerFunction("ST_Point", new GeometryConstructor.ST_Point());
    tEnv.registerFunction("ST_GeomFromText", new GeometryConstructor.ST_GeomFromText());
    tEnv.registerFunction("ST_AsText", new GeometryConstructor.ST_AsText());

    // functions
    tEnv.registerFunction("ST_Transform", new Functions.ST_Transform());

    // predicates
    tEnv.registerFunction("ST_Contains", new Predicates.ST_Contains());

    // glink functions
    tEnv.registerFunction("GL_PointToCSV", new GlinkFunctions.GL_PointToCSV());
  }
}
