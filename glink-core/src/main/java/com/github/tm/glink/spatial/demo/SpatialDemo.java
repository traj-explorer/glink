package com.github.tm.glink.spatial.demo;

import com.github.tm.glink.enums.TextFileSplitter;
import com.github.tm.glink.spatial.datastream.PointDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SpatialDemo {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    PointDataStream pointDataStream = new PointDataStream(
            env, 0, TextFileSplitter.COMMA, false,
            "22.3,33.4,1,hangzhou", "22.4,33.6,2,wuhan");

    pointDataStream.print();

    env.execute();
  }
}
