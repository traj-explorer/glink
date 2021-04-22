package com.github.tm.glink.benchmark.tdrive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TrajectoryReader {

  public static List<Point> read(String path) {
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

  public static void main(String[] args) {
    List<Point> points = read("/media/liebing/p/data/T-drive/release/tdrive_merge.txt");
    System.out.println(points.size());
  }
}
