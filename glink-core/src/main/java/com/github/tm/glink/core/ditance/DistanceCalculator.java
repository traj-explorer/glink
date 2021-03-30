package com.github.tm.glink.core.ditance;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public interface DistanceCalculator {

  double calcDistance(Geometry geom1, Geometry geom2);

  Envelope calcBoxByDist(Geometry geom, double distance);
}
