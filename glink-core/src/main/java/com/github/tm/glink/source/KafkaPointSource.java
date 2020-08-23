package com.github.tm.glink.source;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.github.tm.glink.features.Point;

public class KafkaPointSource {

  private FlinkKafkaConsumer<Point> consumer;

  public KafkaPointSource() {
  }

}
