package com.github.tm.glink.source;

import com.github.tm.glink.feature.Point;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaPointSource extends FlinkKafkaConsumer<Point> {
  public KafkaPointSource(String topic, DeserializationSchema<Point> valueDeserializer, Properties props) {
    super(topic, valueDeserializer, props);
  }
}
