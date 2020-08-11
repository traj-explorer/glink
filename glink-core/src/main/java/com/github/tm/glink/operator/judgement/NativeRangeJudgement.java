package com.github.tm.glink.operator.judgement;

import com.github.tm.glink.fearures.GeoObject;
import org.locationtech.jts.geom.*;

import java.util.*;

/**
 * @author Yu Liebing
 */
public class NativeRangeJudgement<T extends GeoObject, U extends Geometry> extends RangeJudgementBase<T> {

  private List<U> queryGeometries = new ArrayList<>();
  private GeometryFactory geometryFactory = new GeometryFactory();

  public NativeRangeJudgement(U queryGeometry) {
    queryGeometries.add(queryGeometry);
  }

  public NativeRangeJudgement(Envelope queryWindow) {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
    coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
    coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
    coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
    coordinates[4] = coordinates[0];
    U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
    queryGeometries.add(queryGeometry);
  }

  public NativeRangeJudgement(U... queryGeometries) {
    this.queryGeometries.addAll(Arrays.asList(queryGeometries));
  }

  public NativeRangeJudgement(List<U> queryGeometries) {
    this.queryGeometries.addAll(queryGeometries);
  }

  @Override
  public boolean rangeFilter(T geoObject) {
    for (U queryGeometry : queryGeometries) {
      if (queryGeometry.contains(geoObject.getGeometry(geometryFactory))) {
        return true;
      }
    }
    return false;
  }
}
