package com.github.tm.glink.operator.judgement;


import org.apache.flink.configuration.Configuration;

import com.github.tm.glink.features.ClassfiedGrids;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.index.H3Index;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class H3RangeJudgement<T extends Point, U extends Geometry> extends RangeJudgementBase<T> {

  private List<U> queryGeometries = new ArrayList<>();
  private GeometryFactory geometryFactory = new GeometryFactory();
  private int res;
  private transient List<Long> confirmedIndexes;
  private transient List<Long>  toCheckIndexes;
  private transient H3Index h3Index;

  public H3RangeJudgement(U queryGeometry, int res) {
    queryGeometries.add(queryGeometry);
    this.res = res;
  }

  public H3RangeJudgement(Envelope queryWindow, int res) {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
    coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
    coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
    coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
    coordinates[4] = coordinates[0];
    U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
    queryGeometries.add(queryGeometry);
    this.res = res;
  }

  public H3RangeJudgement(int res, U... queryGeometries) {
    this.queryGeometries.addAll(Arrays.asList(queryGeometries));
    this.res = res;
  }

  public H3RangeJudgement(int res, List<U> queryGeometries) {
    this.queryGeometries.addAll(queryGeometries);
    this.res = res;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    ClassfiedGrids classfiedGrids = new ClassfiedGrids();
    h3Index = new H3Index(res);
    for (U geometry : queryGeometries) {
      classfiedGrids.combine(h3Index.getContainGrids(geometry));
    }
    confirmedIndexes = new ArrayList<>(classfiedGrids.getConfirmedIndexes());
    toCheckIndexes = new ArrayList<>(classfiedGrids.getToCheckIndexes());
  }

  @Override
  public boolean rangeFilter(T geoObject) {
    Long index = h3Index.getIndex(geoObject.getLat(), geoObject.getLng());
    for (U queryGeometry : queryGeometries) {
      if (confirmedIndexes.contains(index)) {
        return true;
      } else if (toCheckIndexes.contains(index)) {
        return queryGeometry.contains(geoObject.getGeometry(geometryFactory));
      } else {
        return false;
      }
    }
    return false;
  }
}
