package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.util.AbstractGeoMesaTableSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.geotools.data.*;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.util.factory.Hints;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sink function for Geomesa.
 *
 * @author Yu Liebing
 * */
@Internal
public class GeoMesaSinkFunction<T>
        extends RichSinkFunction<T>
        implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(GeoMesaSinkFunction.class);
  private GeoMesaDataStoreParam params;
  private AbstractGeoMesaTableSchema schema;
  private GeoMesaSimpleFeatureConverter<T> geomesaSimpleFeatureConverter;
  private transient DataStore dataStore;
  private transient FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter;

  public GeoMesaSinkFunction(GeoMesaDataStoreParam params,
                             AbstractGeoMesaTableSchema schema,
                             GeoMesaSimpleFeatureConverter<T> geomesaSimpleFeatureConverter) {
    this.params = params;
    this.schema = schema;
    this.geomesaSimpleFeatureConverter = geomesaSimpleFeatureConverter;
  }

  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void open(Configuration parameters) throws Exception {
    LOG.info("start open ...");
    dataStore = DataStoreFinder.getDataStore(params.getParams());
    if (dataStore == null) {
      LOG.error("Could not create data store with provided parameters");
      throw new RuntimeException("Could not create data store with provided parameters.");
    }
    SimpleFeatureType tableSft = schema.getSimpleFeatureType();
    String name = tableSft.getTypeName();
    SimpleFeatureType sft = dataStore.getSchema(name);
    if (sft == null) {
      LOG.info("Creating schema: " + DataUtilities.encodeType(tableSft));
      tableSft.getUserData().put("override.reserved.words", true);
      tableSft.getUserData().put("geomesa.indices.enabled", schema.getIndicesInfo());
      dataStore.createSchema(tableSft);
    } else {
      String providedSchema = DataUtilities.encodeType(tableSft);
      String existSchema = DataUtilities.encodeType(sft);
      if (!providedSchema.equals(existSchema)) {
        throw new RuntimeException("GeoMesa schema " + tableSft.getTypeName() + " was already exists, " +
                "but the schema you provided is different with the exists one. You provide " + providedSchema +
                ", exists: " + existSchema);
      }
    }
    featureWriter = dataStore.getFeatureWriterAppend(schema.getSimpleFeatureType().getTypeName(), Transaction.AUTO_COMMIT);
    geomesaSimpleFeatureConverter.open();
    LOG.info("end open.");
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    SimpleFeature sf = geomesaSimpleFeatureConverter.convertToSimpleFeature(value);
    SimpleFeature toWrite = featureWriter.next();
    toWrite.setAttributes(sf.getAttributes());
    ((FeatureIdImpl) toWrite.getIdentifier()).setID(sf.getID());
    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
    toWrite.getUserData().putAll(sf.getUserData());
    featureWriter.write();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    // nothing to do.
  }

  @Override
  public void close() throws Exception {
    if (featureWriter != null) {
      featureWriter.close();
    }
    if (dataStore != null) {
      dataStore.dispose();
    }
  }
}
