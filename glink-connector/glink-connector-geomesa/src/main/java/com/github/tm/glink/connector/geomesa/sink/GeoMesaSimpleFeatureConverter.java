package com.github.tm.glink.connector.geomesa.sink;

import org.apache.flink.annotation.Internal;
import org.opengis.feature.simple.SimpleFeature;

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
}
