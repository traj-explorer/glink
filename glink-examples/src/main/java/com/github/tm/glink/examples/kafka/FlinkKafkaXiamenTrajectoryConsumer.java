package com.github.tm.glink.examples.kafka;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.serialization.FlinkTrajectoryDeSerialize;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class FlinkKafkaXiamenTrajectoryConsumer {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<TrajectoryPoint> consumer = new FlinkKafkaConsumer<>(
            "XiamenTrajectory",
            new FlinkTrajectoryDeSerialize("speed:double;azimuth:int;status:int"),
            props);
    DataStream<TrajectoryPoint> dataStream = env.addSource(consumer);

    dataStream.print();

    env.execute();
  }
}
