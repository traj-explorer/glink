package com.github.tm.glink.examples.geomesa;

import com.github.tm.glink.core.operator.HeatMap;
import com.github.tm.glink.examples.source.CSVXiamenTrajectorySource;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Objects;

public class GeoMesaHeatMap {

    public static void main(String[] args) throws Exception {
        Time window_length = Time.minutes(10);
        int h_level = 16;
        int l_level = 14;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        // 从Resources路径下获取文件路径
        String path = Objects.requireNonNull(GeoMesaHeatMap.class.getClassLoader().getResource("XiamenTrajDataCleaned.csv")).getPath();

        // 生成流，执行热力图计算
        DataStream<TrajectoryPoint> trajDataStream = env.addSource(new CSVXiamenTrajectorySource(path, 100))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrajectoryPoint>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                        .withTimestampAssigner((event, timestamp)->event.getTimestamp()));


        DataStream<Tuple4<String, Long, Timestamp, String>> tileStream = HeatMap.GetHeatMap(trajDataStream, h_level,l_level, window_length);

        // 创建GeoMesa中用于存储HeatMap的表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                "CREATE TABLE Geomesa_HeatMap_Test (\n" +
                        // 定义Schema
                        "pk STRING,\n" +
                        "tile_id BIGINT,\n" +
                        "windowEndTime TIMESTAMP(0),\n" +
                        "tile_result STRING,\n" +
                        "PRIMARY KEY (pk) NOT ENFORCED)\n" +
                        // connector的配置设置
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = 'Xiamen-heatmap',\n" +
                        "  'hbase.zookeepers' = 'localhost:2181',\n" +
//                        "  'hbase.zookeepers' = 'u0:2181',\n" +
                        "  'geomesa.indices.enabled' = 'attr:tile_id:windowEndTime',\n" +
                        "  'hbase.catalog' = 'Xiamen-heatmap-test-2'\n" +
                        ")");

        // 将TileStream转换为表，并写入GeoMesa
        Table tileTable = tableEnv.fromDataStream(tileStream);
        tileTable.executeInsert("Geomesa_HeatMap_Test");
        env.execute("heatmap-generating");
    }

}
