package com.github.tm.glink.examples.xiamen;

import com.github.tm.glink.core.operator.HeatMap;
import com.github.tm.glink.core.tile.Pixel;
import com.github.tm.glink.core.tile.PixelResult;
import com.github.tm.glink.core.tile.Tile;
import com.github.tm.glink.core.tile.TileResult;
import com.github.tm.glink.examples.source.CSVXiamenTrajectorySource;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class XiamenHeatMap {

    public static final String ZOOKEEPERS = "localhost:2181";
    public static final String CATALOG_NAME = "Xiamen";
    public static final String SCHEMA_NAME = "Heatmap";
    public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
    public static final int H_LEVEL = 13;
    public static final int L_LEVEL = 12;
    public static final int SPEED_UP = 50;
    public static final long WIN_LEN = 10L;
    public static final int PARALLELSIM = 20;

    public static void main(String[] args) throws Exception {
        Time windowLength = Time.minutes(XiamenHeatMap.WIN_LEN); // 时间窗口长度
        // Drop Heatmap tables in HBase
        new HBaseCatalogCleaner(ZOOKEEPERS).deleteTable(CATALOG_NAME, SCHEMA_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(PARALLELSIM);

        // 模拟流
        DataStream<TrajectoryPoint> trajDataStream = env.addSource(new CSVXiamenTrajectorySource(FILEPATH, SPEED_UP))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrajectoryPoint>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())).setParallelism(1).rebalance();
        HeatMap.GetHeatMap(env, CATALOG_NAME, SCHEMA_NAME, ZOOKEEPERS,trajDataStream, H_LEVEL, L_LEVEL, windowLength, new CountAggregator(), new AddTimeInfoProcess());
    }

    private static class CountAggregator implements AggregateFunction<Tuple2<PixelResult<Integer>, TrajectoryPoint>, Map<Pixel, Tuple2<Integer, HashSet<String>>>, TileResult<Integer>> {

        @Override
        public Map<Pixel, Tuple2<Integer, HashSet<String>>> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, Tuple2<Integer, HashSet<String>>> add(Tuple2<PixelResult<Integer>, TrajectoryPoint> inPiexl, Map<Pixel, Tuple2<Integer, HashSet<String>>> pixelIntegerMap) {
            Pixel pixel = inPiexl.f0.getPixel();
            String carNo = inPiexl.f1.getId();

            try {
                if (!pixelIntegerMap.containsKey(pixel)) {
                    HashSet<String> carNos = new HashSet<>();
                    carNos.add(carNo);
                    pixelIntegerMap.put(pixel, new Tuple2<>(1,carNos));
                } // 该像素第一次出现，进行像素中信息的初始化
                else if(!pixelIntegerMap.get(pixel).f1.contains(carNo)) {
                    pixelIntegerMap.get(pixel).f1.add(carNo);
                    pixelIntegerMap.get(pixel).f0 = pixelIntegerMap.get(pixel).f0  + 1;
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
            for (Map.Entry<Pixel,Tuple2<Integer, HashSet<String>>> entry : pixelIntegerMap.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue().f0));
            }
            return ret;
        }

        @Override
        public Map<Pixel, Tuple2<Integer, HashSet<String>>> merge(Map<Pixel, Tuple2<Integer, HashSet<String>>> acc0, Map<Pixel, Tuple2<Integer, HashSet<String>>> acc1) {
            Map<Pixel, Tuple2<Integer, HashSet<String>>> acc2 = new HashMap<>(acc0);
            acc1.forEach((key, value) -> acc2.merge(key, value, (v1,v2) -> new Tuple2<>(v1.f0+v1.f0, combineSets(v1.f1,v2.f1))));
            return acc1;
        }

        private HashSet<String> combineSets (HashSet<String> v1, HashSet<String> v2) {
            v1.addAll(v2);
            return v1;
        }
    }

    private static class AddTimeInfoProcess extends
            ProcessWindowFunction<TileResult<Integer>, Tuple2<TileResult<Integer>, Timestamp>, Tile, TimeWindow> {
        @Override
        public void process(Tile tile, Context context, Iterable<TileResult<Integer>> elements, Collector<Tuple2<TileResult<Integer>, Timestamp>> out) throws Exception {
            long time = context.window().getEnd();
            Timestamp timestamp = new Timestamp(time);
            out.collect(new Tuple2<>(elements.iterator().next(), timestamp));
        }
    }

}
