package com.github.tm.glink.connector.geomesa.source;

import org.apache.flink.annotation.Internal;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;

/**
 * A converter used to converts the Geomesa {@link org.opengis.feature.simple.SimpleFeature} into an object in Glink system.
 * @param <T> type of output Glink object.
 *
 * @author Yu Liebing
 */
@Internal
public interface GeoMesaGlinkObjectConverter<T> extends Serializable {

  /**
   * Initialization method for the function. It is called once before conversion method.
   */
  void open();

  /**
   * Converts the Geomesa {@link SimpleFeature} into an object in Glink system.
   * @param sf The simple feature object to be converted.
   * @return The target Glink object.
   */
  T convertToFlinkObj(SimpleFeature sf);

  /**
   * To transfer a field in simple feature to a Glink object.
   * @param offsetInSchema The offset in the schema contributing {@link SimpleFeatureType}.
   * @param record The record which will be transformed from the simple feature data.
   * @param sf Source simple feature.
   */
  T setGlinkObjectField(int offsetInSchema, T record, SimpleFeature sf);
}
