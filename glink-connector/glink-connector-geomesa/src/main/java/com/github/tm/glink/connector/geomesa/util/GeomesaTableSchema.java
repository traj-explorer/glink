package com.github.tm.glink.connector.geomesa.util;

import org.apache.flink.table.api.TableSchema;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;

/**
 * Helps to specify a Geomesa Table's schema.
 * */
public class GeomesaTableSchema implements Serializable {

  private static final long serialVersionUID = 1L;

  private SimpleFeatureType schema;

  public SimpleFeatureType getSchema() {
    return schema;
  }

  public static GeomesaTableSchema fromTableSchema(TableSchema tableSchema) {

    return null;
  }
}
