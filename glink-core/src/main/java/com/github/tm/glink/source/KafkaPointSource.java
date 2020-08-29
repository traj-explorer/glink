package com.github.tm.glink.source;

import com.github.tm.glink.features.Point;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaPointSource {

  private FlinkKafkaConsumer<Point> consumer;

  public KafkaPointSource() {
  }

}
