package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.connector.geomesa.options.param.GeomesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.util.GeomesaTableSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * Geomesa table sink implementation.
 *
 * @author Yu Liebing
 * */
@Internal
public class GeomesaDynamicTableSink implements DynamicTableSink {

  private final GeomesaDataStoreParam param;
  private final GeomesaTableSchema geomesaTableSchema;

  public GeomesaDynamicTableSink(GeomesaDataStoreParam param, GeomesaTableSchema geomesaTableSchema) {
    this.param = param;
    this.geomesaTableSchema = geomesaTableSchema;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    // UPSERT mode
    ChangelogMode.Builder builder = ChangelogMode.newBuilder();
    for (RowKind kind : requestedMode.getContainedKinds()) {
      if (kind != RowKind.UPDATE_BEFORE) {
        builder.addContainedKind(kind);
      }
    }
    return builder.build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    GeomesaSinkFunction<RowData> sinkFunction = new GeomesaSinkFunction<>(
            param, geomesaTableSchema, new RowDataToSimpleFeatureConverter(geomesaTableSchema));
    return SinkFunctionProvider.of(sinkFunction);
  }

  @Override
  public DynamicTableSink copy() {
    return new GeomesaDynamicTableSink(param, geomesaTableSchema);
  }

  @Override
  public String asSummaryString() {
    return "Geomesa";
  }
}
