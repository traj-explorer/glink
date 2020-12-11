package com.github.tm.glink.connector.geomesa.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Set;

public class HBaseConfigOption extends GeomesaConfigOption {
  public static final ConfigOption<String> HBASE_CATALOG = ConfigOptions
          .key("hbase.catalog")
          .stringType()
          .noDefaultValue()
          .withDescription("Catalog table name");

  @Override
  public Set<ConfigOption<?>> getRequiredOptions() {
    Set<ConfigOption<?>> set = super.getRequiredOptions();
    set.add(HBASE_CATALOG);
    return set;
  }
}
