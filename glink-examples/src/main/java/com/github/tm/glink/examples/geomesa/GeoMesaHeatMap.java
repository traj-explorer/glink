package com.github.tm.glink.examples.geomesa;

import com.github.tm.glink.core.operator.HeatMap;
import com.github.tm.glink.core.tile.TileGrid;
import com.github.tm.glink.examples.source.CSVXiamenTrajectorySource;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.LinkedList;

public class GeoMesaHeatMap {


    public static void main(String[] args) throws Exception {
        Time window_length = Time.minutes(10); // 时间窗口长度
        int h_level = 18;
        int l_level = 12;  // 默认将设置为0
        int speed_up = 120;
        String catalog = "Xiamen_HeatMap";
        String zookeepers = "u0:2181,u1:2181,u2:2181"; // 管理HBase的Zookeeper地址
        String file_path = "/mnt/hgfs/disk/dcic/data/origin/XiaMen2019Duanwu.csv";
//        String zookeepers = "localhost:2181"; // 管理HBase的Zookeeper地址
//        String file_path = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";

        // Drop Heatmap tables in HBase
        HBaseCatalogCleaner.clean("Xiamen_HeatMap");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);

        LinkedList<TileGrid[]> tileGrids_input = getTileGrids(h_level, l_level);

        // 广播流，广播规则并且创建 broadcast state
        DataStream<TileGrid[]> tileGridsStream = env.fromCollection(tileGrids_input);
        MapStateDescriptor<String,TileGrid[]> tileGridsDescriptor = new MapStateDescriptor<String ,TileGrid[]>("TileGridsDescriptor",BasicTypeInfo.STRING_TYPE_INFO,TypeInformation.of(TileGrid[].class));
        BroadcastStream<TileGrid[]> ruleBroadcastStream = tileGridsStream.broadcast(tileGridsDescriptor);

        // 生成流，执行热力图计算
        DataStream<TrajectoryPoint> trajDataStream = env.addSource(new CSVXiamenTrajectorySource(file_path, speed_up))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrajectoryPoint>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                        .withTimestampAssigner((event, timestamp)->event.getTimestamp()));
        DataStream<Tuple4<String, Long, Timestamp, String>> tileStream = HeatMap.GetHeatMap(trajDataStream, h_level,l_level, window_length, ruleBroadcastStream);

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
    private static LinkedList<TileGrid[]> getTileGrids (int h_level, int l_level) {
        LinkedList<TileGrid[]> tileGrids_input = new LinkedList<>();
        int length = h_level-l_level+1;
        TileGrid[] tileGrids = new TileGrid[length];
        int i = length;
        int j = h_level;
        while (i > 0) {
            tileGrids[i-1] = new TileGrid(j);
            i--;
            j--;
        }
        tileGrids_input.add(tileGrids);
        return tileGrids_input;
    }
}
