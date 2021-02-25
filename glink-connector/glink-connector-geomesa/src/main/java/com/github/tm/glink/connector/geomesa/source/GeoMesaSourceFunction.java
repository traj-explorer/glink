package com.github.tm.glink.connector.geomesa.source;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.geotools.data.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

/**
 * A simple GeoMesa source function.
 *
 * @author Yu Liebing
 * */
public class GeoMesaSourceFunction<T> extends RichSourceFunction<T> implements CheckpointedFunction {

  private boolean isCanceled;

  private GeoMesaDataStoreParam geoMesaDataStoreParam;
  private GeoMesaTableSchema geoMesaTableSchema;
  private GeoMesaRowConverter<T> geoMesaRowConverter;

  private transient DataStore dataStore;
  private transient FeatureReader<SimpleFeatureType, SimpleFeature> featureReader;

  public GeoMesaSourceFunction(GeoMesaDataStoreParam param,
                               GeoMesaTableSchema schema,
                               GeoMesaRowConverter<T> geoMesaRowConverter) {
    this.geoMesaDataStoreParam = param;
    this.geoMesaTableSchema = schema;
    this.geoMesaRowConverter = geoMesaRowConverter;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

  }

  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void open(Configuration parameters) throws Exception {
    dataStore = DataStoreFinder.getDataStore(geoMesaDataStoreParam.getParams());
    if (dataStore == null) {
      throw new RuntimeException("Could not create data store with provided parameters.");
    }
    SimpleFeatureType providedSft = geoMesaTableSchema.getSchema();
    String typeName = providedSft.getTypeName();
    SimpleFeatureType sft = dataStore.getSchema(typeName);
    if (sft == null) {
      throw new RuntimeException("GeoMesa schema doesn't exist, create it first.");
    } else {
      String providedSchema = DataUtilities.encodeType(providedSft);
      String existsSchema = DataUtilities.encodeType(sft);
      if (!providedSchema.equals(existsSchema)) {
        throw new RuntimeException("GeoMesa schema " + sft.getTypeName() + " was already exists, " +
                "but the schema you provided is different with the exists one. You provide " + providedSchema +
                ", exists: " + existsSchema);
      }
    }
    featureReader = dataStore.getFeatureReader(new Query(typeName, Filter.INCLUDE), Transaction.AUTO_COMMIT);
  }

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    while (!isCanceled) {
      while (featureReader.hasNext()) {
        SimpleFeature sf = featureReader.next();
        T rowData = geoMesaRowConverter.convertToRow(sf);
        sourceContext.collect(rowData);
      }
    }
  }

  @Override
  public void cancel() {
    isCanceled = true;
  }
}
