package com.github.tm.glink.benchmark.tdrive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.*;
import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Yu Liebing
 * */
public class BeijingDistrictReader {

  public static List<Geometry> read(String path) {
    System.out.println("reading...");
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

  public static void main(String[] args) {
    List<Geometry> geometries = read("/media/liebing/p/data/beijing_district/beijing_district.csv");
    IntSummaryStatistics statistics = geometries
            .stream()
            .mapToInt(Geometry::getNumPoints)
            .summaryStatistics();
    System.out.println(statistics);
  }
}
