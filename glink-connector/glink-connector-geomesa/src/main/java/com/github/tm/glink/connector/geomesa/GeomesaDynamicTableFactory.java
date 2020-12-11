package com.github.tm.glink.connector.geomesa;

import com.github.tm.glink.connector.geomesa.options.GeomesaConfigOption;
import com.github.tm.glink.connector.geomesa.options.GeomesaConfigOptionFactory;
import com.github.tm.glink.connector.geomesa.options.GeomesaHBaseParam;
import com.github.tm.glink.connector.geomesa.options.HBaseConfigOption;
import com.github.tm.glink.connector.geomesa.sink.GeomesaDynamicTableSink;
import com.github.tm.glink.connector.geomesa.source.GeomesaDynamicTableSource;
import com.github.tm.glink.connector.geomesa.util.GeomesaTableSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Geomesa connector factory.
 *
 * @author Yu Liebing
 * */
public class GeomesaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  private static final String IDENTIFIER = "geomesa-hbase";

  private GeomesaConfigOption geomesaConfigOption = GeomesaConfigOptionFactory.createGeomesaConfigOption("hbase");

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    TableFactoryHelper helper = createTableFactoryHelper(this, context);
    helper.validate();
    TableSchema tableSchema = context.getCatalogTable().getSchema();
    // get geomesa datastore params
    GeomesaHBaseParam param = new GeomesaHBaseParam();
    param.setHBaseCatalog(helper.getOptions().get(HBaseConfigOption.HBASE_CATALOG));
    // convert flink table schema to geomesa schema
    GeomesaTableSchema geomesaTableSchema = GeomesaTableSchema.fromTableSchema(tableSchema);

    return new GeomesaDynamicTableSink(param, geomesaTableSchema);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    TableFactoryHelper helper = createTableFactoryHelper(this, context);
    helper.validate();
    TableSchema tableSchema = context.getCatalogTable().getSchema();

    return new GeomesaDynamicTableSource();
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>(geomesaConfigOption.getRequiredOptions());
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return new HashSet<>(geomesaConfigOption.getOptionalOptions());
  }
}
