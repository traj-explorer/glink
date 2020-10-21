package com.github.tm.glink.examples.mapmatching;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.serialization.FlinkKafkaSerializeSchema;
import com.github.tm.glink.features.serialization.FlinkTrajectoryDeSerialize;
import com.github.tm.glink.mapmathcing.MapMatcher;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
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
   * --road-properties-path /resources/mapmathcing/xiamen.properties
   * --mode viz/throughput
   * */
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool globalParam = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(globalParam);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<TrajectoryPoint> consumer = new FlinkKafkaConsumer<>(
            "map-matching-origin", new FlinkTrajectoryDeSerialize(), props);
    DataStream<TrajectoryPoint> dataStream = env.addSource(consumer);
    DataStream<TrajectoryPoint> matchResult = MapMatcher.mapMatch(dataStream);

    if (globalParam.get("mode").equals("viz")) {
      matchResult
              .map(r -> {
                String res = "{\"time\": %d, \"id\": \"%s\", \"point\": \"POINT (%f %f)\"}";
                Properties attributes = r.getAttributes();
                double lat = (double) attributes.get("lat");
                double lng = (double) attributes.get("lng");
                return String.format(res, r.getTimestamp(), r.getId() + "-" + r.getPid(), lng, lat);
              })
              .addSink(new FlinkKafkaProducer("localhost:9092", "map-matching-result-viz", new SimpleStringSchema()));

      matchResult.print();
    } else {
      String schema = "roadLat:double;roadLng:double;roadId:long";
      FlinkKafkaProducer<TrajectoryPoint> producer = new FlinkKafkaProducer<TrajectoryPoint>(
              "map-matching-result-throughput",
              new FlinkKafkaSerializeSchema("map-matching-result-throughput", schema),
              props,
              FlinkKafkaProducer.Semantic.NONE
      );
      matchResult.addSink(producer);
    }

    env.execute("Map matching");
  }
}
