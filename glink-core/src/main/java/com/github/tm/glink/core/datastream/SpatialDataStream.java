package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.format.TextFormatMap;
import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * @author Yu Liebing
 */
public class SpatialDataStream<T extends Geometry> {

  /**
   * The stream execution environment
   * */
  protected StreamExecutionEnvironment env;

  protected GeometryType geometryType;

  /**
   * The origin flink @{code DataStream}, be private so the subclasses will not see
   * and need to maintain their own @{code DataStream}.
   *
   * In the @{code SpatialDataStream}, generic type @{code T} is a subclass of @{code Geometry}.
   * If it has non-spatial attributes, it can be obtained through @{code Geometry#getUserData}.
   * It is a flink tuple type.
   * */
  private DataStream<T> spatialDataStream;

  protected SpatialDataStream() { }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final int geometryEndOffset,
                           final TextFileSplitter splitter,
                           final GeometryType geometryType,
                           final boolean carryAttributes,
                           final Class<?>[] types,
                           final String... elements) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    this.geometryType = geometryType;
    TextFormatMap<T> textFormatMap = new TextFormatMap<>(
            geometryStartOffset, geometryEndOffset, splitter, geometryType, carryAttributes, types);
    spatialDataStream = env
            .fromElements(elements)
            .flatMap(textFormatMap).returns(geometryType.getTypeInformation());
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final TextFileSplitter splitter,
                           final Class<?>[] types,
                           final String... elements) {
    this(env, geometryStartOffset, geometryStartOffset + 1, splitter, GeometryType.POINT, true, types, elements);
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final int geometryEndOffset,
                           final TextFileSplitter splitter,
                           final GeometryType geometryType,
                           final boolean carryAttributes,
                           final Class<?>[] types,
                           final String csvPath) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    this.geometryType = geometryType;
    TextFormatMap<T> textFormatMap = new TextFormatMap<>(
            geometryStartOffset, geometryEndOffset, splitter, geometryType, carryAttributes, types);
    spatialDataStream = env
            .readTextFile(csvPath)
            .flatMap(textFormatMap).returns(geometryType.getTypeInformation());
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final TextFileSplitter splitter,
                           final Class<?>[] types,
                           final String csvPath) {
    this(env, geometryStartOffset, geometryStartOffset + 1, splitter, GeometryType.POINT, true, types, csvPath);
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final SourceFunction<T> sourceFunction) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    spatialDataStream = env
            .addSource(sourceFunction);
  }

  public SpatialDataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> watermarkStrategy) {
    spatialDataStream = spatialDataStream.assignTimestampsAndWatermarks(watermarkStrategy);
    return this;
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
    try {
      CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
      CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
      final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
      spatialDataStream = spatialDataStream.map(r -> (T) JTS.transform(r, transform));
      return true;
    } catch (FactoryException e) {
      e.printStackTrace();
      return false;
    }
  }

  public IndexedSpatialDataStream<T> index() {
    return new IndexedSpatialDataStream<>(this);
  }

  public TileSpatialDataStream<T> grid(int maxLevel) {
    return new TileSpatialDataStream<>(this, maxLevel);
  }

  public DataStream<T> getDataStream() {
    return spatialDataStream;
  }

  public GeometryType getGeometryType() {
    return geometryType;
  }

  public void print() {
    spatialDataStream
            .map(r -> r + " " + r.getUserData())
            .print();
  }

}
