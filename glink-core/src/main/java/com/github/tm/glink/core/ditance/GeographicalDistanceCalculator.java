package com.github.tm.glink.core.ditance;

import com.github.tm.glink.core.util.GeoUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public class GeographicalDistanceCalculator implements DistanceCalculator {

  @Override
  public double calcDistance(Geometry geom1, Geometry geom2) {

    return GeoUtils.calcDistance(geom1, geom2);
  }

  @Override
  public Envelope calcBoxByDist(Geometry geom, double distance) {
    return GeoUtils.calcBoxByDist(geom.getCentroid(), distance);
  }
}
