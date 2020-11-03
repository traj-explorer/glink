package com.github.tm.glink.spatial.datastream;

import com.github.tm.glink.enums.TextFileSplitter;
import com.github.tm.glink.serialize.GlinkSerializerRegister;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Yu Liebing
 */
public class SpatialDataStream<T extends Geometry> {

  /**
   * The stream execution environment
   * */
  protected StreamExecutionEnvironment env;

  protected boolean useIndex;

  protected DataStream<T> spatialDataStream;

  protected DataStream<Tuple2<Long, T>> indexedSpatialDataStream;

  public SpatialDataStream(StreamExecutionEnvironment env,
                           int geometryOffset,
                           TextFileSplitter splitter,
                           boolean carryAttributes,
                           String... elements) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;

  }

  /**
   * CRS transform.
   *
   * @param sourceEpsgCRSCode the source epsg CRS code
   * @param targetEpsgCRSCode the target epsg CRS code
   * @param lenient consider the difference of the geodetic datum between the two coordinate systems,
   *                if {@code true}, never throw an exception "Bursa-Wolf Parameters Required", but not
   *                recommended for careful analysis work
   *
   * @return true, if successful
   */
  @SuppressWarnings("checkstyle:MethodName")
  public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode, boolean lenient) {
    return false;
  }

  public SpatialDataStream<T> rangeQuery(Envelope rangeQueryWindow) {
    return null;
  }

  public void print() {
    spatialDataStream
            .map(p -> p.getUserData() == null ? p : p + "\t" + p.getUserData())
            .print();
  }

}
