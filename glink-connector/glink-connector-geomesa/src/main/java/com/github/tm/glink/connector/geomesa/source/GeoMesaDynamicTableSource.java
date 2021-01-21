package com.github.tm.glink.connector.geomesa.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;

/**
 * Geomesa table source implementation.
 *
 * @author Yu Liebing
 * */
public class GeoMesaDynamicTableSource implements
        ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    int[][] keys = lookupContext.getKeys();
    System.out.println("++++lookup");
    return null;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    System.out.println("+++scan");
    GeoMesaSourceFunction<RowData> sourceFunction = new GeoMesaSourceFunction<>();
    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public DynamicTableSource copy() {
    return null;
  }

  @Override
  public String asSummaryString() {
    return "GeoMesa";
  }

  @Override
  public boolean supportsNestedProjection() {
    // planner doesn't support nested projection push down yet.
    return false;
  }

  @Override
  public void applyProjection(int[][] ints) {

  }
}
