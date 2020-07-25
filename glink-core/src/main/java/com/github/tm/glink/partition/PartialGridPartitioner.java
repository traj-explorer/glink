package com.github.tm.glink.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author Yu Liebing
 */
public class PartialGridPartitioner implements Partitioner<Integer> {
  @Override
  public int partition(Integer key, int numPartitions) {
    return key % numPartitions;
  }
}
