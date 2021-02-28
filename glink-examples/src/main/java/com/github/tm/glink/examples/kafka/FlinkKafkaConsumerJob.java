package com.github.tm.glink.examples.kafka;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.serialization.FlinkPointSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class FlinkKafkaConsumerJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<Point> consumer = new FlinkKafkaConsumer<>(
            "point", new FlinkPointSchema(), props);
    DataStream<Point> dataStream = env.addSource(consumer);

    dataStream.print();

    env.execute();
  }
}
