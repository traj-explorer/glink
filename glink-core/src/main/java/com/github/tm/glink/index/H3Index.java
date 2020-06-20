package com.github.tm.glink.index;

import com.uber.h3core.H3Core;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class H3Index extends GridIndex {

  private H3Core h3Core;
  private int res;

  public H3Index(int res) {
    try {
      h3Core = H3Core.newInstance();
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.res = res;
  }

  @Override
  public long getIndex(double lat, double lng) {
    return h3Core.geoToH3(lat, lng, res);
  }

  @Override
  public long getParent(long index) {
    return h3Core.h3ToParent(index, res - 1);
  }

  @Override
  public List<Long> getChildren(long index) {
    return h3Core.h3ToChildren(index, res);
  }

  @Override
  public List<Long> getContainGrids(Geometry geometry) {
    return null;
  }
}
