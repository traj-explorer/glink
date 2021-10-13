package com.github.tm.glink.core.areadetect;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.spatial4j.distance.DistanceUtils;

public class TargetEnvelop {
  private final Envelope envelope;

  /**
   *
   * @param lat 最左下角点的纬度
   * @param lng 最左下角点的经度
   * @param latGridSize 纬度方向网格大小（km）
   * @param lngGridSize 经度方向网格大小（km）
   * @param latNum 纬度方向网格数量
   * @param lngNum 经度方向网格数量
   */
  public TargetEnvelop(double lng, double lat, double lngGridSize, double latGridSize, long lngNum, long latNum){
    double lngOffset = lngGridSize * DistanceUtils.KM_TO_DEG;
    double latOffset = latGridSize * DistanceUtils.KM_TO_DEG;
    envelope = new Envelope(lng - lngOffset / 2, lngNum * lngOffset - lngOffset / 2, lat - latOffset / 2, (latNum - 1/2) *latOffset);
  }

  public Envelope getEnvelope(){
    return this.envelope;
  }
}
