package com.github.tm.glink.connector.geomesa.source;

import com.github.tm.glink.connector.geomesa.util.AbstractGeoMesaTableSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

/**
 * @author Wang Haocheng
 * @date 2021/5/11 - 4:03 下午
 */
public class SimpleFeatureToGeometryConverter implements GeoMesaGlinkObjectConverter<Geometry> {

  private AbstractGeoMesaTableSchema tableSchema;

  public SimpleFeatureToGeometryConverter(AbstractGeoMesaTableSchema geoMesaTableSchema) {
    tableSchema = geoMesaTableSchema;
  }

  @Override
  public void open() { }

  @Override
  public Geometry convertToFlinkObj(SimpleFeature sf) {
    Geometry geom = null;
    // 1 使用sf的几何初始化geom
    int geomIndex = tableSchema.getIndexedGeometryFieldIndex();
    geom = setGlinkObjectField(geomIndex, geom, sf);
    // 2 将sf属性赋值给geom
    for (int i = 0; i < sf.getAttributeCount(); i++) {
      if (i != geomIndex) {
        geom = setGlinkObjectField(i, geom, sf);
      }
    }
    return geom;
  }

  @Override
  public Geometry setGlinkObjectField(int offsetInSchema, Geometry record, SimpleFeature sf) {
    int offsetInUserData = getOffsetInUserData(offsetInSchema);
    if (offsetInUserData == -1) {
      // if the offsetInSchema is the geometry field index.
      Geometry res = (Geometry) sf.getDefaultGeometry();
      res.setUserData(Tuple.newInstance(sf.getAttributeCount() - 1));
      return res;
    } else {
      Tuple userData = (Tuple) record.getUserData();
      userData.setField(tableSchema.getFieldValue(offsetInSchema, sf), offsetInUserData);
      return record;
    }
  }

  private int getOffsetInUserData(int offsetInSchema) {
    int indexedGeometryOffset = tableSchema.getIndexedGeometryFieldIndex();
    if (indexedGeometryOffset == offsetInSchema) return -1;
    if (offsetInSchema < indexedGeometryOffset) {
      return offsetInSchema;
    } else {
      return offsetInSchema - 1;
    }
  }
}
