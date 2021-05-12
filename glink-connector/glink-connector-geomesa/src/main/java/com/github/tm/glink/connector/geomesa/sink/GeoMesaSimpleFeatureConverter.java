package com.github.tm.glink.connector.geomesa.sink;

import org.apache.flink.annotation.Internal;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;

/**
 * A converter used to converts the input record into Geomesa {@link org.opengis.feature.simple.SimpleFeature}.
 * @param <T> type of input record.
 *
 * @author Yu Liebing
 */
@Internal
public interface GeoMesaSimpleFeatureConverter<T> extends Serializable {

  /**
   * Initialization method for the function. It is called once before conversion method.
   */
  void open();

  /**
   * Converts the input record into Geomesa {@link SimpleFeature}.
   */
  SimpleFeature convertToSimpleFeature(T record);

  /**
   * Get the primary key that will be assigned to the simple feature.
   * <p> It can be a composition from many fields.
   * @param record The record which will be transformed to the simple feature data.
   * @return The primary key string.
   */
  String getPrimaryKey(T record);

  /**
   * To set a field of a simple feature.
   * @param offsetInSchema The offset in the schema contributing {@link SimpleFeatureType}.
   * @param record The record which will be transformed to the simple feature data.
   */
  void setSimpleFeatureField(int offsetInSchema, T record);
}
