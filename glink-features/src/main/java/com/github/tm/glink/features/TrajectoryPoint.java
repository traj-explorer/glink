package com.github.tm.glink.features;

/**
 * @author Yu Liebing
 */
public class TrajectoryPoint extends Point {

  private int pid;

  public TrajectoryPoint() { }

  public TrajectoryPoint(String tid, int pid, double lat, double lng, long timestamp) {
    this(tid, pid, lat, lng, timestamp, 0L);
  }

  public TrajectoryPoint(String tid, int pid, double lat, double lng, long timestamp, long index) {
    this.id = tid;
    this.pid = pid;
    this.lat = getIntLatLng(lat);
    this.lng = getIntLatLng(lng);
    this.timestamp = timestamp;
    this.index = index;
  }

  public int getPid() {
    return pid;
  }

  public void setPid(int pid) {
    this.pid = pid;
  }

  @Override
  public String toString() {
    return String.format("TrajectoryPoint{id=%s, pid=%d, lat=%.07f, lng=%.07f, timestamp=%d, attributes=%s}",
            id, pid, getDoubleLat(), getDoubleLng(), timestamp, attributes);
  }
}
