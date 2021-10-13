package com.github.tm.glink.core.areadetect;

import java.util.Objects;

public class  DetectionUnit<T> {
  protected Long id;
  protected Long timestamp;
  protected Long partition;
  protected T val;

  public DetectionUnit(long id, long timestamp, T val) {
    this.id = id;
    this.timestamp = timestamp;
    this.val = val;
  }

  public DetectionUnit(long id, long timestamp) {
    this.id = id;
    this.timestamp = timestamp;
  }

  public void setPartition(Long partition) {
    this.partition = partition;
  }

  public Long getPartition() {
    return partition;
  }

  public T getVal() {
    return val;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return "UnigridUnit{"
        + "id=" + id
        + ", timestamp=" + timestamp
        + ", val=" + val
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DetectionUnit<?> that = (DetectionUnit<?>) o;
    return Objects.equals(id, that.id) && Objects.equals(timestamp, that.timestamp) && Objects.equals(partition, that.partition) && Objects.equals(val, that.val);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, timestamp, partition, val);
  }
}
