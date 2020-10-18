package com.github.tm.glink.examples.mapmatching;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import com.github.tm.glink.mapmathcing.MapMatcher;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author Yu Liebing
 */
public class MapMatchingJob {

  /**
   * --file-path ./resources/mapmathcing/0a1c60b6ec3cf05f07479a9e64a3dc90.txt
   * --road-properties-path ./resources/mapmathcing/xiamen.properties
   * */
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool globalParam = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(globalParam);

    Schema schema = new Schema.Parser().parse(String.format(AvroTrajectoryPoint.SCHEMA, ""));
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<GenericRecord> consumer = new FlinkKafkaConsumer<>(
            "map-matching-origin", AvroDeserializationSchema.forGeneric(schema), props);
    DataStream<GenericRecord> dataStream = env.addSource(consumer);
    DataStream<TrajectoryPoint> trajectoryDataStream = dataStream.map(AvroTrajectoryPoint::genericToTrajectoryPoint);

    DataStream<TrajectoryPoint> matchResult = MapMatcher.mapMatch(trajectoryDataStream);
    matchResult
            .map(r -> {
              String res = "{\"time\": %d, \"id\": \"%s\", \"point\": \"POINT (%f %f)\"}";
              return String.format(res, r.getTimestamp(), r.getId() + "-" + r.getPid(), r.getLng(), r.getLat());
            })
            .addSink(new FlinkKafkaProducer("localhost:9092", "map-matching-result", new SimpleStringSchema()));

    matchResult.print();
    env.execute("Map matching");
  }
}
