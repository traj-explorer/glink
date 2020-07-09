package com.github.tm.glink.source;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.github.tm.glink.feature.Point;

public class DidiGPSPointPartitionar extends FlinkKafkaPartitioner<Point> {
  @Override
  public int partition(Point point, byte[] bytes, byte[] bytes1, String s, int[] ints) {
    return Math.abs(new String(bytes).hashCode() % ints.length);
  }
}
