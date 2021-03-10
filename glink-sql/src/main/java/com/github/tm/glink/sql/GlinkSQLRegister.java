package com.github.tm.glink.sql;

import com.github.tm.glink.sql.udf.ParseTimestamp;
import com.github.tm.glink.sql.udf.TestFunc;
import com.github.tm.glink.sql.udf.extention.output.GL_PointToCSV;
import com.github.tm.glink.sql.udf.extention.visualization.GL_Heatmap;
import com.github.tm.glink.sql.udf.extention.visualization.GL_Tile;
import com.github.tm.glink.sql.udf.standard.constructor.*;
import com.github.tm.glink.sql.udf.standard.output.ST_AsBinary;
import com.github.tm.glink.sql.udf.standard.output.ST_AsText;
import com.github.tm.glink.sql.udf.standard.relationship.ST_Contains;
import com.github.tm.glink.sql.udf.standard.relationship.ST_Transform;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Yu Liebing
 */
public class GlinkSQLRegister {

  public static void registerUDF(StreamTableEnvironment tEnv) {
    /**
     * standard spatial sql functions
     * */
    // geometry constructors
    tEnv.registerFunction("ST_GeometryFromText", new ST_GeomFromText());
    tEnv.registerFunction("ST_GeomFromText", new ST_GeomFromText());
    tEnv.registerFunction("ST_GeomFromWKT", new ST_GeomFromWKT());
    tEnv.registerFunction("ST_LineFromText", new ST_LineFromText());
    tEnv.registerFunction("ST_MakeLine", new ST_MakeLine());
    tEnv.registerFunction("ST_MakePoint", new ST_MakePoint());
    tEnv.registerFunction("ST_MakePointM", new ST_MakePointM());
    tEnv.registerFunction("ST_MakePolygon", new ST_MakePolygon());
    tEnv.registerFunction("ST_MLineFromText", new ST_MLineFromText());
    tEnv.registerFunction("ST_MPointFromText", new ST_MPointFromText());
    tEnv.registerFunction("ST_MPolyFromText", new ST_MPolyFromText());
    tEnv.registerFunction("ST_Point", new ST_Point());
    tEnv.registerFunction("ST_PointFromText", new ST_PointFromText());
    tEnv.registerFunction("ST_PointFromWKB", new ST_PointFromWKB());
    tEnv.registerFunction("ST_Polygon", new ST_Polygon());
    tEnv.registerFunction("ST_PolygonFromText", new ST_PolygonFromText());

    // geometry outputs
    tEnv.registerFunction("ST_AaBinary", new ST_AsBinary());
    tEnv.registerFunction("ST_AsText", new ST_AsText());

    // geometry relationships
    tEnv.registerFunction("ST_Contains", new ST_Contains());
    tEnv.registerFunction("ST_Transform", new ST_Transform());

    /**
     * glink extended spatial sql functions
     * */
    // geometry output
    tEnv.registerFunction("GL_PointToCSV", new GL_PointToCSV());

    // viz
    tEnv.registerFunction("GL_Tile", new GL_Tile());
    tEnv.registerFunction("GL_Headmap", new GL_Heatmap());

    tEnv.registerFunction("TestFunc", new TestFunc());
    tEnv.registerFunction("ParseTimestamp", new ParseTimestamp());
  }
}
