package com.github.tm.glink.core.serialize;

import com.github.tm.glink.features.Point;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yu Liebing
 */
public class GlinkSerializerRegister {

  public static void registerSerializer(StreamExecutionEnvironment env) {
    env.getConfig().registerTypeWithKryoSerializer(Point.class, PointKryoSerializer.class);
  }
}
