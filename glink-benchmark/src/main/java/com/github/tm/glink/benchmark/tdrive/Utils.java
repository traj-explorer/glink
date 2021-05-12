package com.github.tm.glink.benchmark.tdrive;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Yu Liebing
 * */
public class Utils {

  /**
   * Read t-drive trajectory data from a merged file.
   * */
  public static List<Point> readTrajectory(String path) {
    File file = new File(path);
    if (!file.isFile())
      throw new RuntimeException("Need a file");
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    final GeometryFactory factory = new GeometryFactory();
    List<Point> points = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      points =  br
              .lines()
              .map(line -> {
                String[] list = line.split(",");
                int id = Integer.parseInt(list[0]);
                LocalDateTime time = LocalDateTime.parse(list[1], formatter);
                double lng = Double.parseDouble(list[2]);
                double lat = Double.parseDouble(list[3]);
                Point point = factory.createPoint(new Coordinate(lng, lat));
                Tuple2<Integer, Long> attr = new Tuple2<>(id, time.toInstant(ZoneOffset.of("+8")).toEpochMilli());
                point.setUserData(attr);
                return point;
              })
              .collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return points;
  }

  /**
   * Read Beijing district.
   * */
  public static List<Geometry> readDistrict(String path) {
    File file = new File(path);
    if (!file.isFile())
      throw new RuntimeException("Need a file");
    WKTReader reader = new WKTReader();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      return br
              .lines()
              .map(line -> {
                try {
                  String[] list = line.split(";");
                  int id = Integer.parseInt(list[0]);
                  String name = list[1];
                  Tuple2<Integer, String> attr = new Tuple2<>(id, name);
                  Geometry geometry = reader.read(list[2]);
                  geometry.setUserData(attr);
                  return geometry;
                } catch (ParseException e) {
                  e.printStackTrace();
                }
                return null;
              })
              .collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<>(0);
  }

  public static boolean isGeometryListAttrEqual(List<Geometry> l1, List<Geometry> l2) {
    int len1 = l1.size(), len2 = l2.size();
    if (len1 != len2) return false;
    for (int i = 0; i < len1; ++i) {
      Tuple a1 = (Tuple) l1.get(i).getUserData();
      Tuple a2 = (Tuple) l2.get(i).getUserData();
      if (!a1.equals(a2)) return false;
    }
    return true;
  }
}
