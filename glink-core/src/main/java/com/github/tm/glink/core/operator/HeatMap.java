package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.tile.*;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.core.tile.AvroTileResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.*;

/**
 * @author Wang Haocheng
 */
public class HeatMap {
    /**
     * 以像素空间范围内车辆出现频次为主题的热力图生成方法。
     * 流程为：
     * 1. 将点类型的DataStream转化为携带点要素信息与像素信息的PixelResult。
     * 2. 将PixelResult以Tile为单位分流
     * 3. 分配窗口 —— 根据每张热力图涉及的时间段长度设置WindowAssigner
     * 4. 进行聚合，聚合结果为TileResult。
     * @param geoDataStream 输入
     * @param h_Level 所需要热力图的最深层级
     * @param l_Level  所需要热力图的最浅层级, 默认为0级
     * @return Tuple4中各项含义以此为：TileId-time(主键,string), tile-id(tile编号,long)
     * ,end_time(时间窗口的结束时间戳,Timestamp),tile_result(Tile内的具体数据,String)
     */
    public static void GetHeatMap(
            StreamExecutionEnvironment env,
            String CATALOG_NAME,
            String SCHEMA_NAME,
            String ZOOKEEPERS,
            DataStream<TrajectoryPoint> geoDataStream,
            int h_Level,
            int l_Level,
            Time time_Len,
            AggregateFunction aggregateFunction,
            ProcessWindowFunction processFunction) {
        // init TileGrids of all levels;
        if (l_Level < 0) {
            l_Level = 0;
        }
        if (h_Level > 18) {
            h_Level = 18;
        }
        int finalH_Level = h_Level;
        int finalL_Level = l_Level;
        int length = finalH_Level-finalL_Level+1;
        // Get a data stream mixed by pixels in different levels.
        DataStream<Tuple2<PixelResult<Integer>, TrajectoryPoint>> pixelResultDataStream =
                geoDataStream.flatMap(new RichFlatMapFunction<TrajectoryPoint, Tuple2<PixelResult<Integer>, TrajectoryPoint>>() {
            private transient TileGrid[] tileGrids;
            @Override
            public void open(Configuration conf) {
                int length = finalH_Level-finalL_Level+1;
                tileGrids = new TileGrid[length];
                int i = length;
                int j = finalH_Level;
                while (i > 0) {
                    tileGrids[i-1] = new TileGrid(j);
                    i--;
                    j--;
                }
            }
            @Override
            public void flatMap(TrajectoryPoint value, Collector<Tuple2<PixelResult<Integer>, TrajectoryPoint>> out) throws Exception {
                int i = length;
                while (i > 0) {
                    out.collect(new Tuple2<>(new PixelResult<>(tileGrids[i - 1].getPixel(value.getLat(), value.getLng()), 1), value));
                    i = i - 1;
                }
            }
        });

        DataStream<Tuple4<String, Long, Timestamp, byte[]>> tileStream =   pixelResultDataStream.keyBy(r -> r.f0.getPixel().getTile())
                .window(SlidingEventTimeWindows.of(time_Len,Time.minutes(5)))
                .aggregate(aggregateFunction, processFunction)
                .map(new MapFunction<Tuple2<TileResult<Integer>, Timestamp>, Tuple4<String, Long, Timestamp, byte[]>>() {
                    @Override
                    public Tuple4<String, Long, Timestamp, byte[]> map(Tuple2<TileResult<Integer>, Timestamp> value) throws Exception {
                        return new Tuple4<String, Long, Timestamp, byte[]>(
                                GetPrimaryKey(value), value.f0.getTile().toLong(), value.f1, AvroTileResult.serialize(value.f0));
                    }});

        // 创建GeoMesa中用于存储HeatMap的表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                "CREATE TABLE Geomesa_HeatMap (\n"
                        // 定义Schema
                        + "pk STRING,\n"
                        + "tile_id BIGINT,\n"
                        + "windowEndTime TIMESTAMP(0),\n"
                        + "tile_result BYTES,\n"
                        + "PRIMARY KEY (pk) NOT ENFORCED)\n"
                        // connector的配置设置
                        + "WITH (\n"
                        + "  'connector' = 'geomesa',\n"
                        + "  'geomesa.data.store' = 'hbase',\n"
                        + "  'geomesa.schema.name' = '"+ SCHEMA_NAME +"',\n"
                        + "  'hbase.zookeepers' = '" + ZOOKEEPERS + "',\n"
                        + "  'hbase.catalog' = '" + CATALOG_NAME + "'\n"
                        + ")");
        // 将TileStream转换为表，并写入GeoMesa
        Table tileTable = tableEnv.fromDataStream(tileStream);
        tileTable.executeInsert("Geomesa_HeatMap");
    }



    /**
     * 使用TS编码，T：年月日+bin(5minutes) S:level+xy交叉
     * @param inputTileResult
     * @return
     */
    private static String GetPrimaryKey (Tuple2<TileResult<Integer>, Timestamp> inputTileResult) {
        return String.valueOf(fromTimestampToLong(inputTileResult.f1)) + String.valueOf(inputTileResult.f0.getTile().toLong());
    }

    /**
     * 21 bit length:
     * year - 5 bits - 16 year
     * month- 5 bits - 12 months
     * day  - 6 bits - 31 days
     * hour - 6 bits - 24 hours
     * min  - 5 bits - 12 bins
     * @param timestamp
     * @return
     */
    private static long fromTimestampToLong(Timestamp timestamp) {
        LocalDateTime ldt = timestamp.toLocalDateTime();
        int year_offset = ldt.getYear()-2019;
        int month_offset = ldt.getMonthValue();
        int day_offset = ldt.getDayOfMonth();
        int hour_offset = ldt.getHour();
        int fiveMin_offset = ldt.getMinute()/5;
        return year_offset << 22 | month_offset<<17 | day_offset << 11 | hour_offset << 5 | fiveMin_offset;
    }

    public static void main(String[] args) {
        LocalDateTime ldt = LocalDateTime.of(2019,6,6,15,20);
        int year_offset = ldt.getYear()-2019;
        int month_offset = ldt.getMonthValue();
        int day_offset = ldt.getDayOfMonth();
        int hour_offset = ldt.getHour();
        int fiveMin_offset = ldt.getMinute()/5;
        long val = year_offset << 22 | month_offset<<17 | day_offset << 11 | hour_offset << 5 | fiveMin_offset;
        System.out.println(val);
    }

    /**
     * bytes1在前，bytes2在后
     * @param bytes1
     * @param bytes2
     * @return
     */
    private static Byte[] combineByteArray (byte[] bytes1, byte[] bytes2) {
        Byte[] ret = new Byte[bytes1.length + bytes2.length];
        for (int i = 0; i < bytes1.length;i++) {
            ret[i] = bytes1[i];
        }
        for (int i = 0; i < bytes2.length;i++) {
            ret[i + bytes1.length] = bytes2[i];
        }
        return ret;
    }
}
