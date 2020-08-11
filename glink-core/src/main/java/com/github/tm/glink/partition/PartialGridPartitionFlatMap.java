package com.github.tm.glink.partition;

import com.github.tm.glink.fearures.BoundingBox;
import com.github.tm.glink.fearures.Point;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @author Yu Liebing
 */
public class PartialGridPartitionFlatMap implements FlatMapFunction<Point, Tuple3<Integer, Boolean, Point>> {

  private BoundingBox[] boundingBoxes;
  private BoundingBox[] bufferBoundingBoxes;

  public PartialGridPartitionFlatMap(BoundingBox boundingBox, double distance, int row, int col) {
    this(
            boundingBox.getMinLat(),
            boundingBox.getMaxLat(),
            boundingBox.getMinLng(),
            boundingBox.getMaxLng(),
            distance,
            row,
            col);
  }

  public PartialGridPartitionFlatMap(
          double minLat,
          double maxLat,
          double minLng,
          double maxLng,
          double distance,
          int row,
          int col) {
    boundingBoxes = new BoundingBox[row * col];
    bufferBoundingBoxes = new BoundingBox[row * col];
    double latStep = (maxLat - minLat) / row;
    double lngStep = (maxLng - minLng) / col;
    int c = 0;
    for (int i = 0; i < row; ++i) {
      double startLat = minLat + latStep * i;
      double endLat = minLat + latStep * (i + 1);
      for (int j = 0; j < col; ++j) {
        double startLng = minLng + lngStep * j;
        double endLng = minLng + lngStep * (j + 1);
        boundingBoxes[c] = new BoundingBox(startLat, endLat, startLng, endLng);
        bufferBoundingBoxes[c] = boundingBoxes[c].getBufferBBox(distance);
        c++;
      }
    }
  }

  @Override
  public void flatMap(Point point, Collector<Tuple3<Integer, Boolean, Point>> collector) throws Exception {
    for (int i = 0; i < bufferBoundingBoxes.length; ++i) {
      if (bufferBoundingBoxes[i].contains(point)) {
//        System.out.println(Thread.currentThread().getId() + ", " + point);
        if (boundingBoxes[i].contains(point)) {
          collector.collect(new Tuple3<>(i, true, point));
        } else {
          collector.collect(new Tuple3<>(i, false, point));
        }
      }
    }
  }
}
