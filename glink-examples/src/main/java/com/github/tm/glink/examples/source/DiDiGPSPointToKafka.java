package com.github.tm.glink.examples.source;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import com.github.tm.glink.feature.Point;
import com.github.tm.glink.source.DidiGPSPointPartitionar;
import com.github.tm.glink.source.DidiGPSPointSource;
import com.github.tm.glink.util.PointSchema;

import java.util.Properties;


public class DiDiGPSPointToKafka {

  private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
  public static final String DIDI_GPS_POINTS_TOPIC = "DiDiGPSPoints01";
  public static final String FILE_PATH = "processedFile";

  @SuppressWarnings("checkstyle:WhitespaceAfter")
  public static void main(String[] args) throws Exception {
    final int servingSpeedFactor = 1; // events of 10 minute are served in 1 second.
    Properties props = new Properties();
    props.setProperty("boostrap.server", LOCAL_KAFKA_BROKER);
    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // start the data generator
    KeyedStream<Point, String> rides = env.addSource(new DidiGPSPointSource(FILE_PATH, servingSpeedFactor, 1477955400L))
        .keyBy(Point::getId);

    rides.addSink(new FlinkKafkaProducer011<Point>(DIDI_GPS_POINTS_TOPIC, new PointSchema(), props, java.util.Optional.of(new DidiGPSPointPartitionar())));
    rides.print();
    env.execute("DidiGPSPointToKafka");
  }
}
