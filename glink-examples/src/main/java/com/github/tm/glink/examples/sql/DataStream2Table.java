package com.github.tm.glink.examples.sql;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.sql.Adapter;
import com.github.tm.glink.sql.util.Schema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class DataStream2Table {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
            env, 0, 1, TextFileSplitter.COMMA, GeometryType.POINT, true,
            Schema.types(Integer.class, String.class),
            "22.3,33.4,1,hangzhou", "22.4,33.6,2,wuhan");

    Table table = Adapter.toTable(tEnv, pointDataStream,
            Schema.names("point", "id", "name"),
            Schema.types(Point.class, Integer.class, String.class));

    Table result = tEnv.sqlQuery("select * from " + table);

    tEnv.toAppendStream(result, Row.class).print();

    env.execute();
  }
}
