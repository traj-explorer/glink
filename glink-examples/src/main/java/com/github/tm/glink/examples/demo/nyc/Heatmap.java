package com.github.tm.glink.examples.demo.nyc;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import com.github.tm.glink.connector.geomesa.sink.GeoMesaSinkFunction;
import com.github.tm.glink.connector.geomesa.util.GeoMesaStreamTableSchema;
import com.github.tm.glink.connector.geomesa.util.GeoMesaType;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.datastream.TileDataStream;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.enums.TileAggregateType;
import com.github.tm.glink.core.tile.Pixel;
import com.github.tm.glink.core.tile.PixelResult;
import com.github.tm.glink.core.tile.TileResult;
import com.github.tm.glink.examples.demo.xiamen.AvroStringTileResultToSimpleFeatureConverter;
import com.github.tm.glink.sql.util.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.locationtech.jts.geom.Point;

import java.time.Duration;
import java.util.*;

/**
 * @author Wang Haocheng
 * @date 2021/6/17 - 10:38 上午
 */
public class Heatmap {
    public static final int PARALLELISM = 4;
    public static final String ZOOKEEPERS = "u0:2181,u1:2181,u2:2181";
    public static final String KAFKA_BOOSTRAP_SERVERS = "u0:9092";
    public static final String KAFKA_GROUP_ID = "TWOJOBSB";
    public static final String CATALOG_NAME = "NYC";
    public static final String TILE_SCHEMA_NAME = "Heatmap";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(PARALLELISM);
        env.disableOperatorChaining();

        // Kafka properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BOOSTRAP_SERVERS);
        props.put("zookeeper.connect", ZOOKEEPERS);
        props.setProperty("group.id", KAFKA_GROUP_ID);

        // Get heatmap sink function
        Configuration confForTiles = new Configuration();
        confForTiles.setString("geomesa.data.store", "hbase");
        confForTiles.setString("hbase.catalog", CATALOG_NAME);
        confForTiles.setString("geomesa.schema.name", TILE_SCHEMA_NAME);
        confForTiles.setString("hbase.zookeepers", ZOOKEEPERS);
        confForTiles.setString("geomesa.primary.field.name", "id");
        confForTiles.setString("geomesa.indices.enabled", "id");
        List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForTile = new LinkedList<>();
        fieldNamesToTypesForTile.add(new Tuple2<>("pk", GeoMesaType.STRING));
        fieldNamesToTypesForTile.add(new Tuple2<>("tile_id", GeoMesaType.LONG));
        fieldNamesToTypesForTile.add(new Tuple2<>("windowEndTime", GeoMesaType.DATE));
        fieldNamesToTypesForTile.add(new Tuple2<>("tile_result", GeoMesaType.STRING));
        GeoMesaDataStoreParam tileDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
        tileDataStoreParam.initFromConfigOptions(confForTiles);
        GeoMesaStreamTableSchema heatMapSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForTile, confForTiles);
        GeoMesaSinkFunction heatMapSink = new GeoMesaSinkFunction(tileDataStoreParam, heatMapSchema, new AvroStringTileResultToSimpleFeatureConverter(heatMapSchema));


        // Fields: vendor_name, Trip_Pickup_DateTime, Passenger_Count, Trip_Distance, Start_Lon, Start_Lat
        SpatialDataStream<Point> originalDataStream = new SpatialDataStream<Point>(
                env, new FlinkKafkaConsumer<>(KafkaDataProducer.TOPICID, new SimpleStringSchema(), props).setStartFromLatest(),
                1, 2, TextFileSplitter.CSV, GeometryType.POINT, true,
                Schema.types(Long.class, String.class))
                .assignTimestampsAndWatermarks((WatermarkStrategy.<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> ((Tuple) event.getUserData()).getField(0))));

        TileDataStream tileDataStream = new TileDataStream(
                originalDataStream,
                new CountAggregator(),
                TumblingEventTimeWindows.of(Time.hours(1)),
                18,
                12,
                false,
                TileAggregateType.getFinalAggregateFunction(TileAggregateType.SUM));
        tileDataStream.getTileResultDataStream().addSink(heatMapSink);
        env.execute();
    }

    static class CountAggregator implements AggregateFunction<Tuple2<PixelResult<Integer>, Point>, Map<Pixel, Tuple2<Integer, HashSet<String>>>, TileResult<Integer>> {

        @Override
        public Map<Pixel, Tuple2<Integer, HashSet<String>>> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, Tuple2<Integer, HashSet<String>>> add(Tuple2<PixelResult<Integer>, Point> inPiexl, Map<Pixel, Tuple2<Integer, HashSet<String>>> pixelIntegerMap) {
            Pixel pixel = inPiexl.f0.getPixel();
            String carNo = ((Tuple) inPiexl.f1.getUserData()).getField(1);
            try {
                if (!pixelIntegerMap.containsKey(pixel)) {
                    HashSet<String> carNos = new HashSet<>();
                    carNos.add(carNo);
                    pixelIntegerMap.put(pixel, new Tuple2<>(1, carNos));
                } else if (!pixelIntegerMap.get(pixel).f1.contains(carNo)) {
                    pixelIntegerMap.get(pixel).f1.add(carNo);
                    pixelIntegerMap.get(pixel).f0 = pixelIntegerMap.get(pixel).f0 + 1;
                } // 该像素已经出现过，但是车辆尚未在其中出现过。
            } catch (Exception e) {
                e.printStackTrace();
            }
            return pixelIntegerMap;
        }

        @Override
        public TileResult<Integer> getResult(Map<Pixel, Tuple2<Integer, HashSet<String>>> pixelIntegerMap) {
            TileResult<Integer> ret = new TileResult<>();
            ret.setTile(pixelIntegerMap.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel, Tuple2<Integer, HashSet<String>>> entry : pixelIntegerMap.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue().f0));
            }
            return ret;
        }

        @Override
        public Map<Pixel, Tuple2<Integer, HashSet<String>>> merge(Map<Pixel, Tuple2<Integer, HashSet<String>>> acc0, Map<Pixel, Tuple2<Integer, HashSet<String>>> acc1) {
            Map<Pixel, Tuple2<Integer, HashSet<String>>> acc2 = new HashMap<>(acc0);
            acc1.forEach((key, value) -> acc2.merge(key, value, (v1, v2) -> new Tuple2<>(v1.f0 + v1.f0, combineSets(v1.f1, v2.f1))));
            return acc2;
        }

        private HashSet<String> combineSets(HashSet<String> v1, HashSet<String> v2) {
            v1.addAll(v2);
            return v1;
        }
    }

}
