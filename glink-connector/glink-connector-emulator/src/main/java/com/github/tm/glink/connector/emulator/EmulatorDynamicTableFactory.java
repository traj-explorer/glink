package com.github.tm.glink.connector.emulator;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Set;

/**
 * @author Yu Liebing
 * */
public class EmulatorDynamicTableFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "emulator";

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    return null;
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return null;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return null;
  }
}
