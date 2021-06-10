package com.github.tm.glink.connector.geomesa.options.param;

import org.apache.flink.table.api.ValidationException;

/**
 * @author Yu Liebing
 * */
public class GeoMesaDataStoreParamFactory {

  public static GeoMesaDataStoreParam createGeomesaDataStoreParam(String dataStore) {
    if ("hbase".equalsIgnoreCase(dataStore)) {
      return new HBaseDataStoreParam();
    } else if ("kafka".equalsIgnoreCase(dataStore)) {
      return new KafkaDataStoreParam();
    } else {
      throw new ValidationException("Unsupported data store");
    }
  }
}
