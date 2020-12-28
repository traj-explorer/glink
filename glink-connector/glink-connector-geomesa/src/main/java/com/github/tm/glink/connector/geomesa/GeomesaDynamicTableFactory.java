package com.github.tm.glink.connector.geomesa;

import com.github.tm.glink.connector.geomesa.options.GeomesaConfigOption;
import com.github.tm.glink.connector.geomesa.options.GeomesaConfigOptionFactory;
import com.github.tm.glink.connector.geomesa.options.param.GeomesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.options.param.GeomesaDataStoreParamFactory;
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

  private static final String IDENTIFIER = "geomesa";

  private GeomesaConfigOption geomesaConfigOption;

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    // create GeomesaConfigOption based on the datastore
    String dataStore = context.getCatalogTable().getOptions().get(GeomesaConfigOption.GEOMESA_DATA_STORE.key());
    geomesaConfigOption = GeomesaConfigOptionFactory.createGeomesaConfigOption(dataStore);

    TableFactoryHelper helper = createTableFactoryHelper(this, context);
    helper.validate();
    TableSchema tableSchema = context.getCatalogTable().getSchema();
    validatePrimaryKey(tableSchema);
    // get geomesa datastore params
    GeomesaDataStoreParam geomesaDataStoreParam = GeomesaDataStoreParamFactory.createGeomesaDataStoreParam(dataStore);
    geomesaDataStoreParam.initFromConfigOptions(helper.getOptions());
    // convert flink table schema to geomesa schema
    GeomesaTableSchema geomesaTableSchema = GeomesaTableSchema.fromTableSchemaAndOptions(
            tableSchema, helper.getOptions());

    return new GeomesaDynamicTableSink(geomesaDataStoreParam, geomesaTableSchema);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    // TODO: write source function for geomesa
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

  private static void validatePrimaryKey(TableSchema tableSchema) {
    if (!tableSchema.getPrimaryKey().isPresent()) {
      throw new IllegalArgumentException("Geomesa table schema required to define a primary key.");
    }
    tableSchema.getPrimaryKey().ifPresent(k -> {
      if (k.getColumns().size() != 1) {
        throw new IllegalArgumentException("Geomesa only supported one primary key.");
      }
    });
  }
}
