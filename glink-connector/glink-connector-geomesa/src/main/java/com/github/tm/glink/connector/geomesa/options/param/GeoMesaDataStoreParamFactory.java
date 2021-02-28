package com.github.tm.glink.connector.geomesa.options.param;

import org.apache.flink.table.api.ValidationException;

/**
 * @author Yu Liebing
 * */
public class GeoMesaDataStoreParamFactory {

  public static GeoMesaDataStoreParam createGeomesaDataStoreParam(String dataStore) {
    if (dataStore.equalsIgnoreCase("hbase")) {
      return new HBaseDataStoreParam();
    } else {
      throw new ValidationException("Unsupported data store");
    }
  }
}
