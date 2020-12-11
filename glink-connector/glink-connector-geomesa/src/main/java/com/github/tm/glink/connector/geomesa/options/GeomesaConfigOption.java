package com.github.tm.glink.connector.geomesa.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashSet;
import java.util.Set;

public class GeomesaConfigOption {
  public static final ConfigOption<Integer> GEOMESA_QUERY_THREADS = ConfigOptions
          .key("geomesa.query.threads")
          .intType()
          .noDefaultValue()
          .withDescription("The number of threads to use per query");

  public Set<ConfigOption<?>> getRequiredOptions() {
    Set<ConfigOption<?>> set = new HashSet<>();
    return set;
  }

  public Set<ConfigOption<?>> getOptionalOptions() {
    Set<ConfigOption<?>> set = new HashSet<>();
    set.add(GEOMESA_QUERY_THREADS);
    return set;
  }
}
