package com.github.tm.glink.connector.geomesa.source;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.util.GeoMesaSQLTableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Geomesa table source implementation.
 *
 * @author Yu Liebing
 * */
public class GeoMesaDynamicTableSource implements
        ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

  private GeoMesaDataStoreParam param;
  private GeoMesaSQLTableSchema schema;

  public GeoMesaDynamicTableSource(GeoMesaDataStoreParam param, GeoMesaSQLTableSchema schema) {
    this.param = param;
    this.schema = schema;
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    int[][] keys = lookupContext.getKeys();
    checkArgument(keys.length == 1 && keys[0].length == 1,
            "Currently, GeoMesa table can only be lookup by single geometry field.");
    String queryField = schema.getFieldName(keys[0][0]);
    GeoMesaGlinkObjectConverter<RowData> geoMesaGlinkObjectConverter = new SimpleFeatureToRowDataConverter(schema);
    return TableFunctionProvider.of(new GeoMesaRowDataLookupFunction(param, schema, geoMesaGlinkObjectConverter, queryField));
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    GeoMesaGlinkObjectConverter<RowData> geoMesaGlinkObjectConverter = new SimpleFeatureToRowDataConverter(schema);
    GeoMesaSourceFunction<RowData> sourceFunction = new GeoMesaSourceFunction<>(param, schema, geoMesaGlinkObjectConverter);
    return SourceFunctionProvider.of(sourceFunction, true);
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
