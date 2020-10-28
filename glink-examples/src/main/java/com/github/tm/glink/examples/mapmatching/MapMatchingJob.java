package com.github.tm.glink.examples.mapmatching;

import com.github.tm.glink.examples.utils.KafkaUtils;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.serialization.FlinkTrajectoryPointSchema;
import com.github.tm.glink.mapmathcing.MapMatcher;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
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

    String brokerList = globalParam.get("broker-list");
    String originTopic = globalParam.get("origin-topic");
    String resultTopic = globalParam.get("result-topic");
    int resultTopicPartitions = Integer.parseInt(globalParam.get("result-topic-partitions"));

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    if (KafkaUtils.isTopicExists(resultTopic, props)) {
      System.out.println("Result topic already exists, deleting...");
      KafkaUtils.deleteTopic(resultTopic, props);
      System.out.println("Create result topic...");
      Map<String, String> configs = new HashMap<>();
      configs.put("log.message.timestamp.type", "LogAppendTime");
      KafkaUtils.createTopic(resultTopic, resultTopicPartitions, 1, props, configs);
    }

    FlinkKafkaConsumer<TrajectoryPoint> consumer = new FlinkKafkaConsumer<>(
            originTopic, new FlinkTrajectoryPointSchema(), props);
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
              .addSink(new FlinkKafkaProducer<>(brokerList, resultTopic, new SimpleStringSchema()));

      matchResult.print();
    } else {
      String schema = "roadLat:double;roadLng:double;roadId:long";
      matchResult.addSink(new FlinkKafkaProducer<>(brokerList, resultTopic,
              new FlinkTrajectoryPointSchema(schema)));
    }

    env.execute("Map matching");
  }
}
