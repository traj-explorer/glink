package com.github.tm.glink.examples.sql.geomesa;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Wang Haocheng
 * @date 2021/3/12 - 10:08 上午
 */
public class XiamenTrajRawDataSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        GlinkSQLRegister.registerUDF(tEnv);

        // create a geomesa source table
        tEnv.executeSql(
                "CREATE TABLE XiamenTraj_RawData (\n" +
                        "point2 STRING,\n" +
                        "status INTEGER,\n" +
                        "speed DOUBLE,\n" +
                        "azimuth INTEGER,\n" +
                        "times TIMESTAMP(0),\n" +
                        " carNo STRING,\n" +
                        " pid STRING,\n" +
                        " PRIMARY KEY (pid) NOT ENFORCED)\n" +
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = 'Xiamen_Point_et',\n" +
                        "  'geomesa.spatial.fields' = 'point2:Point',\n" +
                        "  'hbase.zookeepers' = 'localhost:2181',\n" +
                        "  'hbase.catalog' = 'Xiamen_Point'\n" +
                        ")");

        Table result = tEnv.sqlQuery("SELECT * FROM XiamenTraj_RawData AS A");
        // print the result to the console
        tEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}
