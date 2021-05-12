package com.github.tm.glink.connector.geomesa;

import com.github.tm.glink.connector.geomesa.options.GeoMesaConfigOption;
import com.github.tm.glink.connector.geomesa.options.GeoMesaConfigOptionFactory;
import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import com.github.tm.glink.connector.geomesa.sink.GeoMesaDynamicTableSink;
import com.github.tm.glink.connector.geomesa.source.GeoMesaDynamicTableSource;
import com.github.tm.glink.connector.geomesa.util.GeoMesaSQLTableSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * Geomesa connector factory.
 *
 * @author Yu Liebing
 * */
public class GeoMesaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  private static final String IDENTIFIER = "geomesa";

  private GeoMesaConfigOption geomesaConfigOption;

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    // create GeomesaConfigOption based on the datastore
    String dataStore = context.getCatalogTable().getOptions().get(GeoMesaConfigOption.GEOMESA_DATA_STORE.key());
    geomesaConfigOption = GeoMesaConfigOptionFactory.createGeomesaConfigOption(dataStore);

    TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    TableSchema tableSchema = context.getCatalogTable().getSchema();
    validatePrimaryKey(tableSchema);
    // get geomesa datastore params
    GeoMesaDataStoreParam geomesaDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam(dataStore);
    geomesaDataStoreParam.initFromConfigOptions(helper.getOptions());
    // convert flink table schema to geomesa schema
    GeoMesaSQLTableSchema geomesaTableSchema = new GeoMesaSQLTableSchema(tableSchema, helper.getOptions());

    return new GeoMesaDynamicTableSink(geomesaDataStoreParam, geomesaTableSchema);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    String dataStore = context.getCatalogTable().getOptions().get(GeoMesaConfigOption.GEOMESA_DATA_STORE.key());
    geomesaConfigOption = GeoMesaConfigOptionFactory.createGeomesaConfigOption(dataStore);

    TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    TableSchema tableSchema = context.getCatalogTable().getSchema();
    validatePrimaryKey(tableSchema);
    // get geomesa datastore params
    GeoMesaDataStoreParam geoMesaDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam(dataStore);
    geoMesaDataStoreParam.initFromConfigOptions(helper.getOptions());
    // convert flink table schema to geomesa schema
    GeoMesaSQLTableSchema geoMesaTableSchema = new GeoMesaSQLTableSchema(tableSchema, helper.getOptions());

    return new GeoMesaDynamicTableSource(geoMesaDataStoreParam, geoMesaTableSchema);
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
      if (1 != k.getColumns().size()) {
        throw new IllegalArgumentException("Geomesa only supported one primary key.");
      }
    });
  }
}
