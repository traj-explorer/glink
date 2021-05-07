package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.connector.geomesa.util.GeoMesaTableSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

/**
 * 将{@link org.locationtech.jts.geom.Point}转为{@link org.opengis.feature.simple.SimpleFeature}.
 * @author Wang Haocheng
 * @date 2021/4/30 - 7:46 下午
 */
public class PointToSimpleFeatureConverter implements GeoMesaSimpleFeatureConverter<Point>{

    private GeoMesaTableSchema geomesaTableSchema;
    private transient SimpleFeatureBuilder builder;
    private String spatialFieldName;
    private Integer primaryKeyIndexInUserData;

    /**
     * @param geomesaTableSchema 用于Sink的GeoMesaTable的Schema，pk对应的type必须为Notnull。
     * @param primaryKeyIndex geomesaTableSchema中primary key的index
     */
    public PointToSimpleFeatureConverter(GeoMesaTableSchema geomesaTableSchema) {
        this.geomesaTableSchema = geomesaTableSchema;
        this.primaryKeyIndexInUserData = geomesaTableSchema.getPrimaryKeyIndexInUserData();
    }

    @Override
    public void open() {
        builder = new SimpleFeatureBuilder(geomesaTableSchema.getSchema());
        // 仅有一个spatial field
        spatialFieldName = geomesaTableSchema.getSpatialFieldNames()[0];
    }

    @Override
    public SimpleFeature convertToSimpleFeature(Point point) {
        Tuple userData = (Tuple)point.getUserData();
        // 按照TableSchema顺序构建Simple feature
        int colSize = geomesaTableSchema.getFieldNum();
        int attrOffset = 0;
        for (int i = 0; i < colSize; i++) {
            String thisFieldName = geomesaTableSchema.getFieldName(i);
            if (thisFieldName.equalsIgnoreCase(spatialFieldName)) {
                builder.set(geomesaTableSchema.getFieldName(i), point.getCentroid());
            } else {
                builder.set(geomesaTableSchema.getFieldName(i), userData.getField(attrOffset));
                attrOffset ++;
            }
        }
        return builder.buildFeature(userData.getField(primaryKeyIndexInUserData));
    }
}
