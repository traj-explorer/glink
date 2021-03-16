package com.github.tm.glink.examples.geomesa;

import com.github.tm.glink.core.operator.HeatMap;
import com.github.tm.glink.examples.source.CSVXiamenTrajectorySource;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
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
        Time window_length = Time.minutes(30); // 时间窗口长度
        int h_level = 16;
        int l_level = 13;  // 默认将设置为0
        int speed_up = 120;
        String catalog = "Xiamen_HeatMap";
        String zookeepers = "localhost:2181"; // 管理HBase的Zookeeper地址
        String file_path = Objects.requireNonNull(GeoMesaHeatMap.class.getClassLoader()
                .getResource("XiamenTrajDataCleaned.csv")).getPath(); // 默认从Resources中获取

        // Drop Heatmap tables in HBase
        HBaseCatalogCleaner.clean("XiamXiamen_HeatMap");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        // 生成流，执行热力图计算
        DataStream<TrajectoryPoint> trajDataStream = env.addSource(new CSVXiamenTrajectorySource(file_path, speed_up))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TrajectoryPoint>forBoundedOutOfOrderness(Duration.ofMinutes(2)).withTimestampAssigner((event, timestamp)->event.getTimestamp()));

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
                        "  'hbase.zookeepers' = '"+ zookeepers +"',\n" +
                        "  'geomesa.indices.enabled' = 'attr:tile_id:windowEndTime',\n" +
                        "  'hbase.catalog' = '"+ catalog +"'\n" +
                        ")");

        // 将TileStream转换为表，并写入GeoMesa
        Table tileTable = tableEnv.fromDataStream(tileStream);
        tileTable.executeInsert("Geomesa_HeatMap_Test");
        env.execute("heatmap-generating");
    }
}
