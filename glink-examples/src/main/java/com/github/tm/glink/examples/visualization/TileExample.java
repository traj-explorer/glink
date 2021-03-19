package com.github.tm.glink.examples.visualization;

import com.github.tm.glink.sql.util.Schema;
import com.github.tm.glink.core.datastream.TileResultDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TextFileSplitter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.locationtech.jts.geom.Point;

import java.time.Duration;
import java.util.Properties;

/**
 * @author Yu Liebing
 */
public class TileExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(1L);

    SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
            env, 2, TextFileSplitter.COMMA,
            Schema.types(Integer.class, String.class, Double.class, Integer.class, Long.class, Integer.class),
            "/home/liebing/Code/javaworkspace/glink/glink-examples/src/main/resources/xiamen");
    pointDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Point>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner(new SerializableTimestampAssigner<Point>() {
              @Override
              public long extractTimestamp(Point t, long l) {
                Tuple attributes = (Tuple) t.getUserData();
                return attributes.getField(4);
              }
            }));

//    TileResultDataStream<Integer> gridDataStream = pointDataStream
//            .grid(2).gridSum(TumblingEventTimeWindows.of(Time.seconds(1)));

//    gridDataStream.print();

//    Properties props = new Properties();
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//    gridDataStream.sinkToKafka("tile-test", props);

    env.execute();
  }
}
