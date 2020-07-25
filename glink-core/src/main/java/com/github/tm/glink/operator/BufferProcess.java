package com.github.tm.glink.operator;

import com.github.tm.glink.feature.BoundingBox;
import com.github.tm.glink.feature.Point;
import com.github.tm.glink.operator.process.NativeBufferProcess;
import com.github.tm.glink.partition.PartialGridPartitionFlatMap;
import com.github.tm.glink.partition.PartialGridPartitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class BufferProcess {
  public static DataStream<List<Point>> bufferProcess(
          DataStream<Point> geoDataStream,
          BoundingBox boundingBox,
          int row,
          int col,
          double distance,
          int windowSize,
          boolean useIndex,
          int indexRes) {
    int partitionNum = geoDataStream.getParallelism();
    if (useIndex) {
      return null;
    } else {
      return geoDataStream
              .flatMap(new PartialGridPartitionFlatMap(boundingBox, distance, row, col))
//              .partitionCustom(new PartialGridPartitioner(), 0)
              .keyBy(r -> r.f0)
              .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
              .apply(new NativeBufferProcess(distance));
    }
  }
}
