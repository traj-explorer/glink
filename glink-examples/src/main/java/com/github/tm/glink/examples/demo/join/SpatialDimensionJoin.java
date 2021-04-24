package com.github.tm.glink.examples.demo.join;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.examples.demo.common.BeijingKafkaDeserializationSchema;
import com.github.tm.glink.examples.demo.common.TDriveKafkaDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.locationtech.jts.geom.*;

import java.util.Properties;

/**
 * A demo of how to perform spatial dimension join on t-drive dataset with beijing district.
 *
 * @author Yu Liebing
 * */
public class SpatialDimensionJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String bootstrapServers = parameterTool.get("bootstrap-server");
    String trajectoryTopic = parameterTool.get("t-topic");
    String districtTopic = parameterTool.get("d-topic");
    String outputTopic = parameterTool.get("o-topic");

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create t-drive trajectory SpatialDataStream
    SpatialDataStream<Point> spatialDataStream1 = new SpatialDataStream<>(
            env, new FlinkKafkaConsumer<>(trajectoryTopic, new TDriveKafkaDeserializationSchema(), props));
    // create beijing district BroadcastSpatialDataStream
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 = new BroadcastSpatialDataStream<>(
            env, new FlinkKafkaConsumer<>(districtTopic, new BeijingKafkaDeserializationSchema(), props));

    spatialDataStream1.spatialDimensionJoin(
            spatialDataStream2,
            TopologyType.N_CONTAINS,  // district contains trajectory point
            ((point, geometry) -> {
              Tuple2<Integer, Long> pointAttr = (Tuple2<Integer, Long>) point.getUserData();
              Tuple2<Integer, String> distinctAttr = (Tuple2<Integer, String>) geometry.getUserData();
              return String.format("%d,%d,%s,%d,%s", pointAttr.f0, pointAttr.f1, point, distinctAttr.f0, distinctAttr.f1);
            }),
            new TypeHint<String>() { })
            .addSink(new FlinkKafkaProducer<>(outputTopic, new SimpleStringSchema(), props));

    env.execute("T-Drive Spatial Dimension Join");
  }
}
