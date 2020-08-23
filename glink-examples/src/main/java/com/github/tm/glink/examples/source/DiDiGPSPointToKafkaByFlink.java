package com.github.tm.glink.examples.source;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.github.tm.glink.examples.util.PointSchema;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.source.DidiGPSPointPartitionar;
import com.github.tm.glink.source.DidiGPSPointSource;

import java.util.Properties;


public class DiDiGPSPointToKafkaByFlink {

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

    rides.addSink(new FlinkKafkaProducer(DIDI_GPS_POINTS_TOPIC, new PointSchema(), props, java.util.Optional.of(new DidiGPSPointPartitionar())));
    rides.print();
    env.execute("DidiGPSPointToKafka");
  }
}
