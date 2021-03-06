package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.tile.*;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Haocheng
 */
public class HeatMap {
    private static TileGrid[] tileGrids;
    private static final Logger LOG = LoggerFactory.getLogger(HeatMap.class);


    /**
     * 以像素空间范围内数据出现频次为主题的热力图生成方法。
     * 流程为：
     * 1. 将点类型的DataStream转化为携带点要素信息与像素信息的PixelResult。
     * 2. 将PixelResult以Tile为单位分流
     * 3. 分配窗口 —— 根据每张热力图涉及的时间段长度设置WindowAssigner
     * 4. 进行聚合，聚合结果为TileResult。
     * @param geoDataStream 输入
     * @param h_level 所需要热力图的最深层级
     * @param l_level  所需要热力图的最浅层级, 默认为0级
     * @return Tuple4中各项含义以此为：TileId-time(主键,string), tile-id(tile编号,long)
     * ,end_time(时间窗口的结束时间戳,Timestamp),tile_result(Tile内的具体数据,String)
     */
    public static DataStream<Tuple4<String, Long, Timestamp, String>> GetHeatMap(
            DataStream<TrajectoryPoint> geoDataStream,
            int h_level,
            int l_level,
            Time time_len) {
        // init TileGrids of all levels;
        if (l_level < 0) {
            l_level = 0;
            LOG.warn("The given lower level:" + l_level +" is lower than 0, has been set to 0");
        }
        if (h_level > 18) {
            h_level = 18;
            LOG.warn("The given higher level:" + h_level +" is higher than 18, has been set to 18");
        }
        int finalH_level = h_level;
        int finalL_level = l_level;
        initTileGrids(finalH_level, finalL_level);

        // Get a data stream mixed by pixels in different levels.
        DataStream<PixelResult<Integer>> pixelResultDataStream = geoDataStream
                .flatMap(new FlatMapFunction<TrajectoryPoint, PixelResult<Integer>>() {
                    @Override
                    public void flatMap(TrajectoryPoint value, Collector<PixelResult<Integer>> out) throws Exception {
                        int i = finalH_level +1- finalL_level;
                        while (i > 0) {
                            out.collect(new PixelResult<Integer>(tileGrids[i-1].getPixel(value.getLat(), value.getLng()), 1));
                            i--; }
                    }
                });
        return  pixelResultDataStream.keyBy(r -> r.getPixel().getTile())
                .window(TumblingEventTimeWindows.of(time_len))
                .aggregate(new CountAggregator(), new AddTimeInfoProcess())
                .map(new MapFunction<Tuple2<TileResult<Integer>, Timestamp>, Tuple4<String, Long, Timestamp, String>>() {
                    @Override
                    public Tuple4<String, Long, Timestamp, String> map(Tuple2<TileResult<Integer>, Timestamp> value) throws Exception {
                        return new Tuple4<String, Long, Timestamp, String>(
                                GetPrimaryString(value), value.f0.getTile().toLong(), value.f1, value.f0.toString());
                    }});
    }

    public static DataStream<Tuple4<String, Long,  Timestamp, String>> GetHeatMap(DataStream<TrajectoryPoint> geoDataStream, int level, Time time_len) {
        return GetHeatMap(geoDataStream, level, 0, time_len);
    }

    private static class CountAggregator
            implements AggregateFunction<PixelResult<Integer>, Map<Pixel, Integer>, TileResult<Integer>>{
        @Override
        public Map<Pixel, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, Integer> add(PixelResult<Integer> TrajectoryPointPixelResult, Map<Pixel, Integer> pixelIntegerMap) {
            Pixel pixel = TrajectoryPointPixelResult.getPixel();
            if(pixelIntegerMap.containsKey(pixel)) {
                Integer new_val =  pixelIntegerMap.get(pixel) + 1;
                pixelIntegerMap.put(pixel, new_val);
            }
            else {
                pixelIntegerMap.put(pixel, 1);
            }
            return pixelIntegerMap;
        }

        @Override
        public TileResult<Integer> getResult(Map<Pixel, Integer> pixelIntegerMap) {
            TileResult<Integer> ret = new TileResult<>();
            ret.setTile(pixelIntegerMap.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel,Integer> entry : pixelIntegerMap.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue()));
            }
            return ret;
        }

        @Override
        public Map<Pixel, Integer> merge(Map<Pixel, Integer> acc0, Map<Pixel, Integer> acc1) {
            acc1.forEach((key, value)->acc1.merge(key, value, Integer::sum));
            return acc1;
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

    private static void initTileGrids (int hlevel, int llevel) {
        int length = hlevel-llevel+1;
        tileGrids = new TileGrid[length];
        int i = length;
        int j = hlevel;
        while (i > 0) {
            tileGrids[i-1] = new TileGrid(j);
            i--;
            j--;
        }
    }
}
