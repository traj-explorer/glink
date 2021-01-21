package com.github.tm.glink.core.serialize;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class GlinkSerializerRegister {

  public static void registerSerializer(StreamExecutionEnvironment env) {
    env.getConfig().registerTypeWithKryoSerializer(Point.class, PointKryoSerializer.class);
  }
}
