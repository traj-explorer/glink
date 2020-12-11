package com.github.tm.glink.connector.geomesa.options;

public class GeomesaHBaseParam extends GeomesaParam {
  private final String HBASE_CATALOG = "hbase.catalog";

  public void setHBaseCatalog(String value) {
    params.put(HBASE_CATALOG, value);
  }
}
