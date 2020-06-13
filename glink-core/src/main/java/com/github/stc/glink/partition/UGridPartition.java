package com.github.stc.glink.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author Yu Liebing
 */
public class UGridPartition implements Partitioner {
  @Override
  public int partition(Object o, int i) {
    return 0;
  }
}
