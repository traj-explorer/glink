package com.github.tm.glink.source;

import com.github.tm.glink.fearures.Point;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaPointSource {

  private FlinkKafkaConsumer<Point> consumer;

  public KafkaPointSource() {
  }

}
