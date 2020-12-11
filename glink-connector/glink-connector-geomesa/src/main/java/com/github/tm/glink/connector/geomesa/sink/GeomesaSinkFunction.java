package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.connector.geomesa.options.GeomesaParam;
import com.github.tm.glink.connector.geomesa.util.GeomesaTableSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
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
public class GeomesaSinkFunction<T>
        extends RichSinkFunction<T>
        implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(GeomesaSinkFunction.class);

  private GeomesaParam params;
  private GeomesaTableSchema schema;

  private transient DataStore dataStore;
  private transient FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter;

  public GeomesaSinkFunction(GeomesaParam params, GeomesaTableSchema schema) {
    this.params = params;
    this.schema = schema;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    LOG.info("start open ...");
    dataStore = DataStoreFinder.getDataStore(params.getParams());
    if (dataStore == null) {
      LOG.error("Could not create data store with provided parameters");
      throw new RuntimeException("Could not create data store with provided parameters.");
    }
    featureWriter = dataStore.getFeatureWriterAppend(schema.getSchema().getTypeName(), Transaction.AUTO_COMMIT);
    LOG.info("end open.");
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    System.out.println(value);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    // nothing to do.
  }
}
