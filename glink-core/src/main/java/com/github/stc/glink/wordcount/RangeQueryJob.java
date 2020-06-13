package com.github.stc.glink.wordcount;

import com.github.stc.glink.feature.Point;
import com.github.stc.glink.operator.RangeQuery;
import com.github.stc.glink.source.CSVPointSource;
import com.github.stc.glink.feature.SpatialEnvelope;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yu Liebing
 */
public class RangeQueryJob {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Point> dataStream = env.addSource(new CSVPointSource("/home/lb/input/1.txt"));
    float minX = 114.f;
    float maxX = 115.f;
    float minY = 30.f;
    float maxY = 31.f;
    SpatialEnvelope envelope = new SpatialEnvelope(minX, maxX, minY, maxY);
    DataStream<Point> rangeQueryStream = RangeQuery.spatialRangeQuery(dataStream, envelope);
    rangeQueryStream.print();

    env.execute("test");
  }
}