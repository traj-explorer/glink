package com.github.tm.glink.examples.process;


import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.process.SpatialFilter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;

public class SpatialFilterExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        WKTReader wktReader = new WKTReader();

        SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
                env, "localhost", 9999, new SimplePointFlatMapper());
        Geometry queryGeometry = wktReader.read("POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))");
        DataStream<Point> resultStream = SpatialFilter.filter(pointDataStream, queryGeometry, TopologyType.N_CONTAINS);
        resultStream.print();
        env.execute();
    }

    public static class SimplePointFlatMapper extends RichFlatMapFunction<String, Point> {

        private transient GeometryFactory factory;

        @Override
        public void open(Configuration parameters) throws Exception {
            factory = new GeometryFactory();
        }

        @Override
        public void flatMap(String line, Collector<Point> collector) throws Exception {
            String[] items = line.split(",");
            int id = Integer.parseInt(items[0]);
            double lng = Double.parseDouble(items[1]);
            double lat = Double.parseDouble(items[2]);
            Point p = factory.createPoint(new Coordinate(lng, lat));
            p.setUserData(new Tuple1<>(id));
            collector.collect(p);
        }
    }
}
