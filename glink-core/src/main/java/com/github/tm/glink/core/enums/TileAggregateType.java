package com.github.tm.glink.core.enums;

import com.github.tm.glink.core.tile.Pixel;
import com.github.tm.glink.core.tile.PixelResult;
import com.github.tm.glink.core.tile.TileResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Point;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Wang Haocheng
 * @date 2021/5/8 - 8:56 下午
 */
public enum TileAggregateType implements Serializable {
    COUNT,
    MAX,
    MIN,
    AVG,
    SUM;

    public static AggregateFunction getAggregateFunction(TileAggregateType aggregateType, int aggFieldIndex) {
        switch (aggregateType) {
            case AVG:
                return new AvePixelAgg(aggFieldIndex);
            case MAX:
                return new MaxPixelAgg(aggFieldIndex);
            case MIN:
                return new MinPixelAgg(aggFieldIndex);
            case SUM:
                return new SumPixelAgg(aggFieldIndex);
            case COUNT:
            default:
                return new CountPixelAgg();
        }
    }

    private static class AvePixelAgg <V> implements AggregateFunction<Tuple2<PixelResult<V>, Point>, Map<Pixel, Tuple2<Integer, V>>, TileResult<V>>{

        private int aggFieldIndex;

        private AvePixelAgg(int aggFieldIndex) {
            this.aggFieldIndex = aggFieldIndex;
        }

        @Override
        public Map<Pixel, Tuple2<Integer, V>> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, Tuple2<Integer, V>> add(Tuple2<PixelResult<V>, Point> value, Map<Pixel, Tuple2<Integer, V>> accumulator) {
            Pixel pixel = value.f0.getPixel();
            V val = ((Tuple)value.f1.getUserData()).getField(aggFieldIndex);
            if (!accumulator.containsKey(pixel)) {
                accumulator.put(pixel, new Tuple2<>(1, val));
            } else {
                Tuple2 tuple2 = accumulator.get(pixel);
                Double ave = (Double) tuple2.f1 * (Integer) tuple2.f0 + (Double) val;
                int count = (Integer) tuple2.f0 + 1;
                ave = ave/count;
                accumulator.put(pixel, new Tuple2<>(count,(V) ave));
            }
            return accumulator;
        }

        @Override
        public TileResult<V> getResult(Map<Pixel, Tuple2<Integer, V>> accumulator) {
            TileResult<V> ret = new TileResult<>();
            ret.setTile(accumulator.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel,  Tuple2<Integer, V>> entry : accumulator.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue().f1));
            }
            return ret;
        }

        @Override
        public Map<Pixel, Tuple2<Integer, V>> merge(Map<Pixel, Tuple2<Integer, V>> a, Map<Pixel, Tuple2<Integer, V>> b) {
            Map<Pixel, Tuple2<Integer, V>> c = new HashMap<>(a);
            b.forEach((key, value) -> c.merge(key, value, (v1, v2) -> new Tuple2<>(v1.f0 + v2.f0, (V) (Double) ((Double) v1.f1 + (Double) v2.f1))));
            return c;
        }
    }

    private static class MaxPixelAgg <V> implements AggregateFunction<Tuple2<PixelResult<V>, Point>, Map<Pixel, V>, TileResult<V>>{
        private int aggFieldIndex;

        private MaxPixelAgg(int aggFieldIndex) {
            this.aggFieldIndex = aggFieldIndex;
        }

        @Override
        public Map<Pixel, V> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, V> add(Tuple2<PixelResult<V>, Point> value, Map<Pixel, V> accumulator) {
            Pixel pixel = value.f0.getPixel();
            V val = ((Tuple)value.f1.getUserData()).getField(aggFieldIndex);
            if (!(val instanceof Comparable)) {
                throw new RuntimeException("The aggregate field does not support comparison");
            } else if(!accumulator.containsKey(pixel)) {
                accumulator.put(pixel, val);
            } else if (((Comparable)val).compareTo(accumulator.get(pixel)) > 0) {
                accumulator.put(pixel, val);
            }
            return accumulator;
        }

        @Override
        public TileResult<V> getResult(Map<Pixel,V> accumulator) {
            TileResult<V> ret = new TileResult<>();
            ret.setTile(accumulator.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel, V> entry : accumulator.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue()));
            }
            return ret;
        }

        @Override
        public Map<Pixel, V> merge(Map<Pixel, V> a, Map<Pixel, V> b) {
            Map<Pixel, V> c = new HashMap<>(a);
            b.forEach((key, value) -> c.merge(key, value, (v1, v2) -> {
                Comparable vc1 = (Comparable) v1;
                Comparable vc2 = (Comparable) v2;
                if (vc1.compareTo(vc2) > 0) {
                    return v1;
                } else {
                    return v2;
                }
            }));
            return c;
        }
    }

    private static class MinPixelAgg <V> implements AggregateFunction<Tuple2<PixelResult<V>, Point>, Map<Pixel, V>, TileResult<V>>{

        private int aggFieldIndex;

        private MinPixelAgg(int aggFieldIndex) {
            this.aggFieldIndex = aggFieldIndex;
        }

        @Override
        public Map<Pixel, V> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, V> add(Tuple2<PixelResult<V>, Point> value, Map<Pixel, V> accumulator) {
            Pixel pixel = value.f0.getPixel();
            V val = ((Tuple)value.f1.getUserData()).getField(aggFieldIndex);
            if(!accumulator.containsKey(pixel)) {
                accumulator.put(pixel, val);
            } else if (((Comparable)val).compareTo(accumulator.get(pixel)) < 0) {
                accumulator.put(pixel, val);
            }
            return accumulator;
        }

        @Override
        public TileResult<V> getResult(Map<Pixel,V> accumulator) {
            TileResult<V> ret = new TileResult<>();
            ret.setTile(accumulator.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel, V> entry : accumulator.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue()));
            }
            return ret;
        }

        @Override
        public Map<Pixel, V> merge(Map<Pixel, V> a, Map<Pixel, V> b) {
            Map<Pixel, V> c = new HashMap<>(a);
            b.forEach((key, value) -> c.merge(key, value, (v1, v2) -> {
                Comparable vc1 = (Comparable) v1;
                Comparable vc2 = (Comparable) v2;
                if (vc1.compareTo(vc2) < 0) {
                    return v1;
                } else {
                    return v2;
                }
            }));
            return c;
        }
    }

    private static class SumPixelAgg <V> implements AggregateFunction<Tuple2<PixelResult<V>, Point>, Map<Pixel, V>, TileResult<V>>{

        private int aggFieldIndex;

        private SumPixelAgg(int aggFieldIndex) {
            this.aggFieldIndex = aggFieldIndex;
        }

        @Override
        public Map<Pixel, V> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, V> add(Tuple2<PixelResult<V>, Point> value, Map<Pixel, V> accumulator) {
            Pixel pixel = value.f0.getPixel();
            V val = ((Tuple)value.f1.getUserData()).getField(aggFieldIndex);
            if (accumulator.containsKey(pixel)) {
                Double newVal = (Double) val + (Double) accumulator.get(pixel);
                accumulator.put(pixel, (V) newVal);
            } else {
                accumulator.put(pixel, val);
            }
            return accumulator;
        }

        @Override
        public TileResult<V> getResult(Map<Pixel, V> accumulator) {
            TileResult<V> ret = new TileResult<>();
            ret.setTile(accumulator.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel, V> entry : accumulator.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue()));
            }
            return ret;
        }

        @Override
        public Map<Pixel, V> merge(Map<Pixel, V> a, Map<Pixel, V> b) {
            Map<Pixel, V> c = new HashMap<>(a);
            b.forEach((key, value) -> c.merge(key, value, (v1, v2) -> {
                Double newVd = (Double) v1 + (Double) v2;
                return (V) newVd;
            }));
            return c;
        }
    }

    private static class CountPixelAgg implements AggregateFunction<Tuple2<PixelResult<Integer>, Point>, Map<Pixel, Integer>, TileResult<Integer>>{

        @Override
        public Map<Pixel, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Pixel, Integer> add(Tuple2<PixelResult<Integer>, Point> value, Map<Pixel, Integer> accumulator) {
            Pixel pixel = value.f0.getPixel();
            if (accumulator.containsKey(pixel)) {
                Integer newVal = accumulator.get(pixel) + 1;
                accumulator.put(pixel, newVal);
            } else {
                accumulator.put(pixel, 1);
            }
            return accumulator;
        }

        @Override
        public TileResult<Integer> getResult(Map<Pixel, Integer> accumulator) {
            TileResult<Integer> ret = new TileResult<>();
            ret.setTile(accumulator.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel, Integer> entry : accumulator.entrySet()) {
                ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue()));
            }
            return ret;
        }

        @Override
        public Map<Pixel, Integer> merge(Map<Pixel, Integer> a, Map<Pixel, Integer> b) {
            Map<Pixel, Integer> c = new HashMap<>(a);
            b.forEach((key, value) -> c.merge(key, value, (v1, v2) -> {
                Integer newVd =  v1 +  v2;
                return newVd;
            }));
            return c;
        }
    }

}