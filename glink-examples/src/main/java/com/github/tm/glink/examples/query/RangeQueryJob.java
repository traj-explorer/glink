package com.github.tm.glink.examples.query;

import com.github.tm.glink.feature.Point;
import com.github.tm.glink.operator.RangeQuery;
import com.github.tm.glink.source.CSVPointSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Envelope;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Yu Liebing
 */
public class RangeQueryJob {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, String> param = new HashMap<>();
    param.put("rangeIndex", "all");
    ParameterTool globalParam = ParameterTool.fromMap(param);
    env.getConfig().setGlobalJobParameters(globalParam);

    String path = RangeQueryJob.class.getResource("/trajectory.txt").getPath();
    DataStream<Point> dataStream = env.addSource(new CSVPointSource(path));

    float minX = 114.f;
    float maxX = 115.f;
    float minY = 30.f;
    float maxY = 31.f;

    Envelope envelope = new Envelope(minY, maxY, minX, maxX);
    DataStream<Point> rangeQueryStream = RangeQuery.spatialRangeQuery(dataStream, envelope);
    rangeQueryStream.print();

    env.execute("test");
  }
}