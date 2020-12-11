package com.github.tm.glink.connector.geomesa.options;

public class GeomesaConfigOptionFactory {

  public static GeomesaConfigOption createGeomesaConfigOption(String backendName) {
    if (backendName.equalsIgnoreCase("hbase")) {
      return new HBaseConfigOption();
    } else {
      throw new IllegalArgumentException("Unsupported backend database");
    }
  }
}
