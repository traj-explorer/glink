package com.github.tm.glink.core.enums;

/**
 * Enumeration of topology type.
 *
 * @author Yu Liebing
 * */
public enum TopologyType {
  INTERSECTS,
  P_CONTAINS,
  N_CONTAINS,
  P_WITHIN,
  N_WITHIN,
  WITHIN_DISTANCE,
  P_BUFFER,
  N_BUFFER;

  private double distance;

  public TopologyType distance(double distance) {
    if (this != WITHIN_DISTANCE)
      throw new IllegalArgumentException("Only WITHIN_DISTANCE type can assign distance");

    this.distance = distance;
    return this;
  }

  public double getDistance() {
    if (this != WITHIN_DISTANCE)
      throw new IllegalArgumentException("Only WITHIN_DISTANCE type can get distance");

    return distance;
  }
}
