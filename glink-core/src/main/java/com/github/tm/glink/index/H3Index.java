package com.github.tm.glink.index;

import com.github.tm.glink.feature.ClassfiedGrids;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

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
  public int getRes() {
    return res;
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
  public long getParent(long index, int res) {
    return 0;
  }

  @Override
  public List<Long> getChildren(long index) {
    return null;
  }

  @Override
  public List<Long> getChildren(long index, int res) {
    return null;
  }


  @Override
  public List<Long> kRing(long index, int k) {
    return null;
  }


  public ClassfiedGrids getContainGrids(Geometry geometry){
    ArrayList<Long> all_indexs = (ArrayList<Long>) h3Core.polyfill(geometryToGeoCoordList(geometry),null,res);
    // 将all_indexes分为两类：六边形完全内含于geometry的索引与六边形不完全内含于geometry的索引。
    ClassfiedGrids classfiedIndexes = new ClassfiedGrids();
    for(Long index:all_indexs){
      if(!intersectWith(index,geometry))
        classfiedIndexes.confirmedIndexes.add(index);
      else
        classfiedIndexes.toCheckIndexes.add(index);
    }
    // 找到那些与geometry相交，但中心点并不位于geometry内部的多边形。
    // 在toCheckIndexes中遍历，符合条件的就加入其中，直到所有边缘格网全部处理过了一遍。
    Iterator<Long> iterator = classfiedIndexes.toCheckIndexes.iterator();
    while(iterator.hasNext()){
      Long toCheckIndex = iterator.next();
      List<Long> tempList = h3Core.kRing(toCheckIndex,1);
      for(Long tempIndex : tempList){
        // 如果该索引未包含在all_index中的话。
        if(!classfiedIndexes.confirmedIndexes.contains(tempIndex)&&!classfiedIndexes.toCheckIndexes.contains(tempIndex))
          if(intersectWith(tempIndex,geometry))
            classfiedIndexes.toCheckIndexes.add(tempIndex);
      }
    }
    return classfiedIndexes;
  }


  // 检查一个六边形索引所代表的的六边形网格是否完全处于该多边形内
  private Boolean intersectWith(long index, Geometry geometry){
    List<GeoCoord> boundry=  h3Core.h3ToGeoBoundary(index);

    // List转化为Coordinate[]
    Coordinate[] coorArray = new Coordinate[7];
    int counter = 0;
    for(GeoCoord gc : boundry) {
      coorArray[counter] = new Coordinate(gc.lat,gc.lng);
      counter++;
    }
    coorArray[counter] = coorArray[0];
    // Coordinate[]转化为Polygon
    Polygon hexagon = new Polygon(new LinearRing(coorArray,new PrecisionModel(),4326),null, new GeometryFactory(new PrecisionModel(),4326));
    return hexagon.intersects(geometry);
  }

  private List<GeoCoord> geometryToGeoCoordList(Geometry geometry){
    List<GeoCoord> boundry = new LinkedList<>();
    Coordinate[] coordinates = geometry.getCoordinates();
    for(Coordinate coordinate:coordinates){
      boundry.add(new GeoCoord(coordinate.x,coordinate.y));
    }
    boundry.add(new GeoCoord(coordinates[0].x,coordinates[0].y));
    return boundry;
  }
}