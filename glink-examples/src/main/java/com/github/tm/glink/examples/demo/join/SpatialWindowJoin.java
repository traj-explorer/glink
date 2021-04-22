package com.github.tm.glink.examples.demo.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.locationtech.jts.geom.Point;

import java.time.Duration;
import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class SpatialWindowJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String bootstrapServers = parameterTool.get("bootstrap-servers");
    String streamPath1 = parameterTool.get("t-topic");
    String streamPath2 = parameterTool.get("d-topic");

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    SpatialDataStream<Point> pointSpatialDataStream1 =
            new SpatialDataStream<>(env, streamPath1, new SpatialDimensionJoin.TrajectoryFlatMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);
    SpatialDataStream<Point> pointSpatialDataStream2 =
            new SpatialDataStream<>(env, streamPath2, new SpatialDimensionJoin.TrajectoryFlatMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);

    DataStream<String> dataStream = pointSpatialDataStream1.spatialWindowJoin(
            pointSpatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(10),
            TumblingEventTimeWindows.of(Time.seconds(1200)),
            (point, point2) -> point + ", " + point2,
            new TypeHint<String>() { });
    dataStream.print();
    dataStream.writeAsText("/home/liebing/input/glink/join/result.txt");

    env.execute();
  }
}
