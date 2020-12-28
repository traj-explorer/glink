package com.github.tm.glink.connector.geomesa.options.param;

import org.apache.flink.table.api.ValidationException;

public class GeomesaDataStoreParamFactory {

  public static GeomesaDataStoreParam createGeomesaDataStoreParam(String dataStore) {
    if (dataStore.equalsIgnoreCase("hbase")) {
      return new HBaseDataStoreParam();
    } else {
      throw new ValidationException("Unsupported data store");
    }
  }
}
