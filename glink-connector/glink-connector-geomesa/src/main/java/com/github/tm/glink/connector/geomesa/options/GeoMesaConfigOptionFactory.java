package com.github.tm.glink.connector.geomesa.options;

import org.apache.flink.table.api.ValidationException;

/**
 * @author Yu Liebing
 * */
public class GeoMesaConfigOptionFactory {

  public static GeoMesaConfigOption createGeomesaConfigOption(String dataStore) {
    if (dataStore.equalsIgnoreCase("hbase")) {
      return new HBaseConfigOption();
    } else if (dataStore.equalsIgnoreCase(("kafka"))) {
      return new KafkaConfigOption();
    } else {
      throw new ValidationException("Unsupported data store.");
    }
  }
}
