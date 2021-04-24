package com.github.tm.glink.examples.demo.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.examples.demo.common.TDriveKafkaDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.locationtech.jts.geom.Point;

import java.time.Duration;
import java.util.Properties;

/**
 * A demo of how to perform spatial interval join on t-drive dataset.
 * For simplicity, here we let t-drive's trajectory point itself with its own jon.
 * There should be two different streams in a real-world scenario.
 *
 * @author Yu Liebing
 * */
public class SpatialIntervalJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String bootstrapServers = parameterTool.get("bootstrap-server");
    String trajectoryTopic = parameterTool.get("t-topic");
    String outputTopic = parameterTool.get("o-topic");

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    SpatialDataStream<Point> trajectoryStream1 = new SpatialDataStream<>(
            env, new FlinkKafkaConsumer<>(trajectoryTopic, new TDriveKafkaDeserializationSchema(), props))
            .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);
    SpatialDataStream<Point> trajectoryStream2 = new SpatialDataStream<>(
            env, new FlinkKafkaConsumer<>(trajectoryTopic, new TDriveKafkaDeserializationSchema(), props))
            .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);

    DataStream<String> dataStream = trajectoryStream1.spatialIntervalJoin(
            trajectoryStream2,
            TopologyType.WITHIN_DISTANCE.distance(10),
            Time.seconds(-600),
            Time.seconds(600),
            ((p1, p2) -> {
              Tuple2<Integer, Long> p1Attr = (Tuple2<Integer, Long>) p1.getUserData();
              Tuple2<Integer, Long> p2Attr = (Tuple2<Integer, Long>) p2.getUserData();
              return String.format("%d,%d,%s,%d,%d,%s", p1Attr.f0, p1Attr.f1, p1, p2Attr.f0, p2Attr.f1, p2);
            }),
            new TypeHint<String>() { });
    dataStream.addSink(new FlinkKafkaProducer<>(outputTopic, new SimpleStringSchema(), props));

    env.execute("T-Drive Spatial Interval Join");
  }
}
