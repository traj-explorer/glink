package com.github.tm.glink.connector.geomesa.source;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.geotools.data.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

/**
 * Get geometry instances from GeoMesa.
 * @author Wang Haocheng
 */
public class GeoMesaGeometrySourceFunction<T extends Geometry> extends RichSourceFunction<T> implements CheckpointedFunction {

    private boolean isCanceled;

    private GeoMesaDataStoreParam geoMesaDataStoreParam;
    private GeoMesaTableSchema geoMesaTableSchema;
    private transient DataStore dataStore;
    private transient FeatureReader<SimpleFeatureType, SimpleFeature> featureReader;
    private GeometryFactory geometryFactory = new GeometryFactory();

    public GeoMesaGeometrySourceFunction(GeoMesaDataStoreParam param,
                                 GeoMesaTableSchema schema) {
        this.geoMesaDataStoreParam = param;
        this.geoMesaTableSchema = schema;
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
                sourceContext.collect(convertToGeom(sf));
            }
        }
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }

    private T convertToGeom (SimpleFeature sf) {
        T geom = null;
        Tuple userData = null;
        // 需要跳过几何属性
        if (sf.getDefaultGeometry() != null) {
            geom = (T) sf.getDefaultGeometry();
            userData =  Tuple.newInstance(sf.getAttributeCount() - 1);
        } else {
            geom = (T) new GeometryFactory().createGeometry(null);
            userData =  Tuple.newInstance(sf.getAttributeCount());
        }
        int ind = 0;
        for (int i = 0; i < sf.getAttributeCount(); i++) {
            if (i == geoMesaTableSchema.getDefaultIndexedSpatialFieldIndex()) {
                continue;
            } else {
                userData.setField(sf.getAttribute(i), ind);
                ind ++;
            }
        }
        geom.setUserData(userData);
        return geom;
    }
}
