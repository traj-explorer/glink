package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.connector.geomesa.util.AbstractGeoMesaTableSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

/**
 * 将{@link org.locationtech.jts.geom.Point}转为{@link org.opengis.feature.simple.SimpleFeature}.
 * @author Wang Haocheng
 * @date 2021/4/30 - 7:46 下午
 */
public class PointToSimpleFeatureConverter implements GeoMesaSimpleFeatureConverter<Point> {

  private static final long serialVersionUID = 1L;
  private final AbstractGeoMesaTableSchema geomesaStreamTableSchema;
  private transient SimpleFeatureBuilder builder;

  /**
   * @param geomesaStreamTableSchema 用于Sink的GeoMesaTable的Schema，pk对应的type必须为Notnull。
   */
  public PointToSimpleFeatureConverter(AbstractGeoMesaTableSchema geomesaStreamTableSchema) {
    this.geomesaStreamTableSchema = geomesaStreamTableSchema;
  }

  @Override
  public void open() {
    builder = new SimpleFeatureBuilder(geomesaStreamTableSchema.getSimpleFeatureType());
  }

  @Override
  public SimpleFeature convertToSimpleFeature(Point point) {
    // 按照TableSchema顺序构建Simple feature
    int colSize = geomesaStreamTableSchema.getFieldNum();
    for (int i = 0; i < colSize; i++) {
      setSimpleFeatureField(i, point);
    }
    return builder.buildFeature(getPrimaryKey(point));
  }

  @Override
  public String getPrimaryKey(Point record) {
    Tuple userData = (Tuple) record.getUserData();
    int[] primaryFieldsIndexes = geomesaStreamTableSchema.getPrimaryFieldsIndexes();
    if (1 == primaryFieldsIndexes.length) {
      return userData.getField(getOffsetInUserdata(primaryFieldsIndexes[0]));
    } else {
      // 多个fields进行拼接
      StringBuilder builder = new StringBuilder();
      for (int primaryFieldsIndex : primaryFieldsIndexes) {
        builder.append((String) userData.getField(getOffsetInUserdata(primaryFieldsIndex)));
        builder.append("-");
      }
      return builder.subSequence(0, builder.length() - 1).toString();
    }
  }

  @Override
  public void setSimpleFeatureField(int offsetInSchema, Point record) {
    int geomIndex = geomesaStreamTableSchema.getIndexedGeometryFieldIndex();
    Tuple userData = (Tuple) record.getUserData();
    if (offsetInSchema == geomIndex) {
      builder.set(geomesaStreamTableSchema.getFieldName(offsetInSchema), record.getCentroid());
    } else {
      int offsetInUserData = getOffsetInUserdata(offsetInSchema);
      builder.set(geomesaStreamTableSchema.getFieldName(offsetInSchema), userData.getField(offsetInUserData));
    }
  }

  /**
   * 由于Point的userData中不包括本身用于索引的Geometry，而geomesa schema中用于索引的Geometry并没有和其他的属性字段分开。
   * <p>在想要根据schema的primaryFieldsIndexes获取userdata中对应字段的话，需要对offset进行映射。
   *
   * @param offsetInSchema The offset of a field from schema.
   * @return The offset of the field in user data tuple.
   */
  private int getOffsetInUserdata(int offsetInSchema) {
    int indexedGeometryOffset = geomesaStreamTableSchema.getIndexedGeometryFieldIndex();
    if (offsetInSchema < indexedGeometryOffset) {
      return offsetInSchema;
    } else {
      return offsetInSchema - 1;
    }
  }
}
