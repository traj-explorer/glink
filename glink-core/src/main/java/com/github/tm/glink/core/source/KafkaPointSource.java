package com.github.tm.glink.core.source;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.locationtech.jts.geom.Point;

public class KafkaPointSource {

  private FlinkKafkaConsumer<Point> consumer;

  public KafkaPointSource() {
  }

}
