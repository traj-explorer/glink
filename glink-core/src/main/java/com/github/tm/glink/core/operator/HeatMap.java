package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.tile.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.github.tm.glink.features.Point;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Haocheng
 */
public class HeatMap {
    private static int level;
    private DataStream<Point> pointDataStream;
    private static SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(),4326);
    private static TileGrid tileGrid;

    /**
     * 以像素空间范围内数据出现频次为主题的热力图生成方法。
     * 流程为：
     * 1. 将点类型的DataStream转化为携带点要素信息与像素信息的PixelResult。
     * 2. 将PixelResult以Tile为单位分流
     * 3. 分配窗口 —— 根据每张热力图涉及的时间段长度设置WindowAssigner
     * 4. 进行聚合，聚合结果为TileResult。
     * @param geoDataStream
     * @param level
     * @return Tuple5中各项含义以此为：TileId-time(主键,string), level(等级,int), tile-id(tile编号,long)
     * ,start_time(时间窗口的开始时间,String),tile_result(Tile内的具体数据,String)
     */
    public static DataStream<Tuple5<String, Integer, Long, String, String>> GetHeatMap(
            DataStream<Point> geoDataStream,
            int... level) {
        tileGrid = new TileGrid(10);
        DataStream<PixelResult<Point>> pixelResultDataStream = geoDataStream.map(new MapFunction<Point, PixelResult<Point>>() {
            @Override
            public PixelResult<Point> map(Point r) throws Exception {
                return new PixelResult<Point>(tileGrid.getPixel(r.getLat(),r.getLng(),10),r);
            }
        });
        return  pixelResultDataStream.keyBy(r -> r.getPixel().getTile())
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new CountAggregator(), new AddTimeInfoProcess())
                .map(new MapFunction<Tuple2<TileResult, String>, Tuple5<String, Integer, Long, String, String>>() {
                    @Override
                    public Tuple5<String, Integer, Long, String, String> map(Tuple2<TileResult, String> r) throws Exception {
                        return new Tuple5<String, Integer, Long, String, String>(
                                GetPrimaryString(r),
                                r.f0.getTile().getLevel(),
                                r.f0.getTile().toLong(),
                                r.f1,
                                r.f0.toString());
                    }
                });
    }

    public static class CountAggregator
            implements AggregateFunction<PixelResult<Point>, Map<Pixel, Integer>, TileResult>{

        @Override
        public Map<Pixel, Integer> createAccumulator() {
            return new HashMap<>();
        }
        @Override
        public Map<Pixel, Integer> add(PixelResult<Point> pointPixelResult, Map<Pixel, Integer> pixelIntegerMap) {
            Pixel pixel = pointPixelResult.getPixel();
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
        public TileResult getResult(Map<Pixel, Integer> pixelIntegerMap) {
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

    public static class AddTimeInfoProcess extends
            ProcessWindowFunction<TileResult, Tuple2<TileResult, String>, Tile, TimeWindow> {
        @Override
        public void process(Tile tile, Context context, Iterable<TileResult> elements, Collector<Tuple2<TileResult, String>> out) throws Exception {
            long time = context.window().getEnd();
            String date_info = sdf.format(new Date(time));
            System.out.println(date_info);
            out.collect(new Tuple2<>(elements.iterator().next(), date_info));
        }
    }

    private static String GetPrimaryString (Tuple2<TileResult, String> inputTileResult) {
        StringBuilder builder = new StringBuilder();
        builder.append(inputTileResult.f0.getTile().toLong());
        builder.append(inputTileResult.f1);
        return builder.toString();
    }
}
