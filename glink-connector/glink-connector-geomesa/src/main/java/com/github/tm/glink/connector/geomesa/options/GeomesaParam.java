package com.github.tm.glink.connector.geomesa.options;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GeomesaParam implements Serializable {
  protected final String GEOMESA_QUERY_CACHING = "geomesa.query.caching";

  protected Map<String, String> params = new HashMap<>();

  public void setGeomesaQueryCaching(String value) {
    params.put(GEOMESA_QUERY_CACHING, value);
  }

  public Map<String, String> getParams() {
    return params;
  }
}
