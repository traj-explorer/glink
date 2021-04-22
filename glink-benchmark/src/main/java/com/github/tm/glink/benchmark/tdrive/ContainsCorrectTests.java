package com.github.tm.glink.benchmark.tdrive;

import com.github.tm.glink.core.geom.MultiPolygonWithIndex;
import com.github.tm.glink.core.geom.PolygonWithIndex;
import com.github.tm.glink.core.index.TRTreeIndex;
import com.github.tm.glink.core.index.TreeIndex;
import org.apache.flink.api.java.tuple.Tuple;
import org.locationtech.jts.geom.*;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author Yu Liebing
 * */
public class ContainsCorrectTests {

  private List<Geometry> districts;
  private TreeIndex<Geometry> rTree = new TRTreeIndex<>();
  private TreeIndex<Geometry> rTreeWithIndexedGeometry = new TRTreeIndex<>();

  public ContainsCorrectTests(List<Geometry> districts) {
    this.districts = districts;
    rTree.insert(districts);

    districts.forEach(district -> {
      if (district instanceof Polygon) {
        rTreeWithIndexedGeometry.insert(PolygonWithIndex.fromPolygon((Polygon) district));
      } else if (district instanceof MultiPolygon) {
        rTreeWithIndexedGeometry.insert(MultiPolygonWithIndex.fromMultiPolygon((MultiPolygon) district));
      } else {
        rTreeWithIndexedGeometry.insert(district);
      }
    });
  }

  public List<Geometry> traversalJoin(Point p) {
    List<Geometry> result = new ArrayList<>();
    for (Geometry district : districts) {
      if (district.contains(p)) {
        result.add(district);
      }
    }
    return result;
  }

  public List<Geometry> rTreeJoin(Point p) {
    List<Geometry> result = rTree.query(p);
    result.removeIf(geom -> !geom.contains(p));
    return result;
  }

  public List<Geometry> rTreeWithIndexedGeometryJoin(Point p) {
    List<Geometry> result = rTreeWithIndexedGeometry.query(p);
    result.removeIf(geom -> !geom.contains(p));
    for (Geometry geom : result) {
      geom.contains(p);
    }
    return result;
  }

  public static void main(String[] args) {
    List<Geometry> districts = BeijingDistrictReader.read("/media/liebing/p/data/beijing_district/beijing_district.csv");
    List<Point> points = TrajectoryReader.read("/media/liebing/p/data/T-drive/release/taxi_log_2008_by_id/1.txt");
    ContainsCorrectTests tests = new ContainsCorrectTests(districts);

    for (Point p : points) {
      List<Geometry> r1 = tests.traversalJoin(p);
      List<Geometry> r2 = tests.rTreeJoin(p);
      List<Geometry> r3 = tests.rTreeWithIndexedGeometryJoin(p);
      if (!Utils.isGeometryListAttrEqual(r1, r2)) {
        System.out.println(p + ": " + "rTreeJoin wrong");
      }
      if (!Utils.isGeometryListAttrEqual(r1, r3)) {
        List<String> names1 = r1.stream().map(geom -> (String) ((Tuple) geom.getUserData()).getField(1)).collect(toList());
        List<String> names3 = r3.stream().map(geom -> (String) ((Tuple) geom.getUserData()).getField(1)).collect(toList());
        System.out.println(p.getX() + "," + p.getY() + "," + names1 + "," + names3);
      }
    }
  }
}
