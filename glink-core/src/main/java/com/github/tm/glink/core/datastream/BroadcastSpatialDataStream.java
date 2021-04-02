package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public class BroadcastSpatialDataStream<T extends Geometry> {

  private DataStream<Tuple2<Boolean, T>> dataStream;

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final String path,
                                    final FlatMapFunction<String, Tuple2<Boolean, T>> flatMapFunction) {
    GlinkSerializerRegister.registerSerializer(env);
    dataStream = env
            .readTextFile(path)
            .flatMap(flatMapFunction);
  }

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final String host,
                                    final int port,
                                    final FlatMapFunction<String, Tuple2<Boolean, T>> flatMapFunction) {
    GlinkSerializerRegister.registerSerializer(env);
    dataStream = env
            .socketTextStream(host, port)
            .flatMap(flatMapFunction);
  }

  public DataStream<Tuple2<Boolean, T>> getDataStream() {
    return dataStream;
  }
}
