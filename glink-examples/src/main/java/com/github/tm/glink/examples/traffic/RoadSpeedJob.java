package com.github.tm.glink.examples.traffic;

import com.github.tm.glink.examples.utils.EventTimeAssigner;
import com.github.tm.glink.features.serialization.FlinkTrajectoryPointSchema;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.traffic.RoadSpeedPipeline;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Real-time road speed calculation.
 *
 * @author Yu Liebing
 * */
public class RoadSpeedJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool globalParam = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(globalParam);

    String brokerList = globalParam.get("broker-list");
    String originTopic = globalParam.get("origin-topic");

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    FlinkKafkaConsumer<TrajectoryPoint> consumer = new FlinkKafkaConsumer<>(
            originTopic, new FlinkTrajectoryPointSchema(), props);
    DataStream<TrajectoryPoint> dataStream = env.addSource(consumer);
    dataStream.assignTimestampsAndWatermarks(new EventTimeAssigner<>(0));

//    DataStream<TrajectoryPoint> matchedStream = MapMatcher.mapMatch(dataStream);

    DataStream<Tuple2<Long, Double>> geoObjectDataStream = RoadSpeedPipeline.roadSpeedPipeline(
            dataStream, 60, 20);
    geoObjectDataStream.print();

    env.execute();
  }
}
