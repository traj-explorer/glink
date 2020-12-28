package com.github.tm.glink.connector.geomesa.options;

import org.apache.flink.table.api.ValidationException;

public class GeomesaConfigOptionFactory {

  public static GeomesaConfigOption createGeomesaConfigOption(String dataStore) {
    if (dataStore.equalsIgnoreCase("hbase")) {
      return new HBaseConfigOption();
    } else {
      throw new ValidationException("Unsupported data store.");
    }
  }
}
