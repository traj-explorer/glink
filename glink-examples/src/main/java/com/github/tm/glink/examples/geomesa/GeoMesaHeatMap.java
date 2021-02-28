package com.github.tm.glink.examples.geomesa;

import com.github.tm.glink.core.operator.HeatMap;
import com.github.tm.glink.core.source.CSVGeoObjectSource;
import com.github.tm.glink.examples.query.KNNQueryJob;
import com.github.tm.glink.examples.source.XiamenOriginHDFSDataSource;
import com.github.tm.glink.features.Point;
import org.apache.flink.api.java.tuple.Tuple5;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class GeoMesaHeatMap {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        // 设置hdfs环境与文件路径
        String path = "/XiamenTraj/taxiGps20190606.csv";
//        String hdfsurl = "hdfs://u0:9000";
        String hdfsurl = "hdfs://localhost:9000";
        // 生成流，执行热力图计算
        DataStream<Point> pointDataStream = env.addSource(new XiamenOriginHDFSDataSource(path, hdfsurl))
                .assignTimestampsAndWatermarks(new KNNQueryJob.EventTimeAssigner(1000));
        DataStream<Tuple5<String,Integer, Long, String, String>> tileStream = HeatMap.GetHeatMap(pointDataStream, 10);

        // 创建GeoMesa中用于存储HeatMap的表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                "CREATE TABLE Geomesa_HeatMap_Test (\n" +
                        // 定义Schema
                        "pk STRING,\n" +
                        "level INTEGER,\n" +
                        "tile_id BIGINT,\n" +
                        "start_time STRING,\n" +
                        "tile_result STRING,\n" +
                        "PRIMARY KEY (pk) NOT ENFORCED)\n" +
                        // connector的配置设置
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = 'Xiamen-heatmap-test-1',\n" +
                        "  'hbase.zookeepers' = 'localhost:2181',\n" +
//                        "  'hbase.zookeepers' = 'u0:2181',\n" +
                        "  'geomesa.indices.enabled' = 'attr:level:tile_id:start_time',\n" +
                        "  'hbase.catalog' = 'Xiamen-heatmap-test-3'\n" +
                        ")");

        // 将TileStream转换为表，并写入GeoMesa
        Table tileTable = tableEnv.fromDataStream(tileStream);
        tileTable.executeInsert("Geomesa_HeatMap_Test");
        env.execute("heatmap-generating");
    }

}
