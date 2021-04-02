package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.tile.*;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
    public static DataStream<Tuple4<String, Long, Timestamp, String>> GetHeatMap(
            DataStream<TrajectoryPoint> geoDataStream,
            int h_Level,
            int l_Level,
            Time time_Len) {
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

        return  pixelResultDataStream.keyBy(r -> r.f0.getPixel().getTile())
                .window(TumblingEventTimeWindows.of(time_Len))
                .aggregate(new CountAggregator(), new AddTimeInfoProcess())
                .map(new MapFunction<Tuple2<TileResult<Integer>, Timestamp>, Tuple4<String, Long, Timestamp, String>>() {
                    @Override
                    public Tuple4<String, Long, Timestamp, String> map(Tuple2<TileResult<Integer>, Timestamp> value) throws Exception {
                        return new Tuple4<String, Long, Timestamp, String>(
                                GetPrimaryString(value), value.f0.getTile().toLong(), value.f1, value.f0.toString());
                    }});
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

    private static String GetPrimaryString (Tuple2<TileResult<Integer>, Timestamp> inputTileResult) {
        StringBuilder builder = new StringBuilder();
        builder.append(inputTileResult.f0.getTile().toLong());
        builder.append(inputTileResult.f1.toString());
        return builder.toString();
    }
}
