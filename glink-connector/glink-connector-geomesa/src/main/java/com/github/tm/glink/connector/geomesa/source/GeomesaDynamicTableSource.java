package com.github.tm.glink.connector.geomesa.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;

/**
 * Geomesa table source implementation.
 *
 * @author Yu Liebing
 * */
public class GeomesaDynamicTableSource implements
        ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    return null;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return null;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return null;
  }

  @Override
  public DynamicTableSource copy() {
    return null;
  }

  @Override
  public String asSummaryString() {
    return null;
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] ints) {

  }
}
