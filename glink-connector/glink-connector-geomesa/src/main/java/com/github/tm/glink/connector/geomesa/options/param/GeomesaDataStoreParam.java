package com.github.tm.glink.connector.geomesa.options.param;

import com.github.tm.glink.connector.geomesa.options.GeomesaConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Geomesa Data Store parameters.
 *
 * @author Yu Liebing
 * */
public class GeomesaDataStoreParam implements Serializable {
  protected Map<String, Serializable> params = new HashMap<>();

  public Map<String, Serializable> getParams() {
    return params;
  }

  public void initFromConfigOptions(ReadableConfig config) {
    config.getOptional(GeomesaConfigOption.GEOMESA_SECURITY_AUTHS)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_SECURITY_AUTHS.key(), v));
    config.getOptional(GeomesaConfigOption.GEOMESA_SECURITY_FORCE_EMPTY_AUTHS)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_SECURITY_FORCE_EMPTY_AUTHS.key(), v));
    config.getOptional(GeomesaConfigOption.GEOMESA_QUERY_AUDIT)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_QUERY_AUDIT.key(), v));
    config.getOptional(GeomesaConfigOption.GEOMESA_QUERY_TIMEOUT)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_QUERY_TIMEOUT.key(), v));
    config.getOptional(GeomesaConfigOption.GEOMESA_QUERY_THREADS)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_QUERY_THREADS.key(), v));
    config.getOptional(GeomesaConfigOption.GEOMESA_QUERY_LOOSE_BOUNDING_BOX)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_QUERY_LOOSE_BOUNDING_BOX.key(), v));
    config.getOptional(GeomesaConfigOption.GEOMESA_STATS_GENERATE)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_STATS_GENERATE.key(), v));
    config.getOptional(GeomesaConfigOption.GEOMESA_QUERY_CACHING)
            .ifPresent(v -> params.put(GeomesaConfigOption.GEOMESA_QUERY_CACHING.key(), v));
  }
}
