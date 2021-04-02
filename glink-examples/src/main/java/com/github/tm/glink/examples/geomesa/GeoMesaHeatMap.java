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

public class GeoMesaHeatMap {
    public static final long WIN_LEN = 10L;
    public static final String ZOOKEEPERS = "localhost:2181";
    public static final String CATALOG_NAME = "Xiamen_HeatMap";
    public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
    public static final int H_LEVEL = 13;
    public static final int L_LEVEL = 12;
    public static final int SPEED_UP = 50;
    public static final int PARALLELSIM = 20;

    public static void main(String[] args) throws Exception {
        Time windowLength = Time.minutes(GeoMesaHeatMap.WIN_LEN); // 时间窗口长度
        // Drop Heatmap tables in HBase
        HBaseCatalogCleaner.clean(GeoMesaHeatMap.CATALOG_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(PARALLELSIM);

        // 生成流，执行热力图计算
        DataStream<TrajectoryPoint> trajDataStream = env.addSource(new CSVXiamenTrajectorySource(FILEPATH, SPEED_UP))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrajectoryPoint>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())).setParallelism(1).rebalance();
        DataStream<Tuple4<String, Long, Timestamp, String>> tileStream = HeatMap.GetHeatMap(trajDataStream, H_LEVEL, L_LEVEL, windowLength);
        // 创建GeoMesa中用于存储HeatMap的表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                "CREATE TABLE Geomesa_HeatMap_Test (\n"
                        // 定义Schem
                        + "pk STRING,\n"
                        + "tile_id BIGINT,\n"
                        + "windowEndTime TIMESTAMP(0),\n"
                        + "tile_result STRING,\n"
                        + "PRIMARY KEY (pk) NOT ENFORCED)\n"
                        // connector的配置设置
                        + "WITH (\n"
                        + "  'connector' = 'geomesa',\n"
                        + "  'geomesa.data.store' = 'hbase',\n"
                        + "  'geomesa.schema.name' = 'Xiamen-heatmap',\n"
                        + "  'hbase.zookeepers' = '" + ZOOKEEPERS + "',\n"
                        + "  'geomesa.indices.enabled' = 'attr:tile_id:windowEndTime',\n"
                        + "  'hbase.catalog' = '" + GeoMesaHeatMap.CATALOG_NAME + "'\n"
                        + ")");
        // 将TileStream转换为表，并写入GeoMesa
        Table tileTable = tableEnv.fromDataStream(tileStream);
        tileTable.executeInsert("Geomesa_HeatMap_Test");
        env.execute("heatmap-generating");
    }
}
