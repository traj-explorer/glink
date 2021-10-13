package com.github.tm.glink.core.areadetect;

import com.github.tm.glink.core.areadetect.unigrid.UAreaDetect;
import com.github.tm.glink.core.index.GeographicalGridIndex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;


import java.time.Duration;

public class AreaDetectTest {

  public static void main(String[] args) throws Exception {
    // some params
    final GeographicalGridIndex index = new GeographicalGridIndex(new TargetEnvelop(0, 0, 0.05, 0.05, 20, 20).getEnvelope(), 0.05, 0.05);
    final TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.minutes(5));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    SingleOutputStreamOperator<DetectionUnit<Double>> source = env
        .addSource(new ExampleDataSource("/Users/haocheng/IdeaProjects/glink/rasters.txt", index))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<DetectionUnit<Double>>forBoundedOutOfOrderness(Duration.ofMinutes(5))
            .withTimestampAssigner((r, timestamp) -> r.getTimestamp()));
    // submit job
    DataStream<Geometry> ds = new UAreaDetect<>(env, windowAssigner, new InterestDetect(1), source, index).process();
    ds.print();
    env.execute();
  }
}
