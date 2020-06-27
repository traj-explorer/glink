package com.github.tm.glink.examples.query;

import com.github.tm.glink.examples.source.CSVDiDiGPSPointSource;
import com.github.tm.glink.feature.Point;
import com.github.tm.glink.feature.QueryPoint;
import com.github.tm.glink.operator.KNNQuery;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author Yu Liebing
 */
public class KNNQueryJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    env.getConfig().setAutoWatermarkInterval(1000L);

    int k = 10;
    QueryPoint queryPoint = new QueryPoint(30.66, 104.05);

    String path = KNNQueryJob.class.getResource("/gps_20161101_0710").getPath();
    DataStream<Point> pointDataStream = env.addSource(new CSVDiDiGPSPointSource(path))
            .assignTimestampsAndWatermarks(new EventTimeAssigner(5000));

    DataStream<Point> knnResult = KNNQuery.pointKNNQuery(
            pointDataStream, queryPoint, k, 50, false, 9);
    knnResult.print();

    env.execute("KNN Query test");
  }

  public static class EventTimeAssigner implements AssignerWithPeriodicWatermarks<Point> {

    private final long maxOutOfOrderness;
    private long currentMaxTimestamp;

    public EventTimeAssigner(long maxOutOfOrderness) {
      this.maxOutOfOrderness  = maxOutOfOrderness;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
      return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Point point, long previousElementTimestamp) {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, point.getTimestamp());
      return point.getTimestamp();
    }
  }
}
