package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.connector.geomesa.util.GeoMesaSQLTableSchema;
import org.apache.flink.table.data.RowData;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

/**
 * An implementation of {@link GeoMesaSimpleFeatureConverter} which converts {@link RowData} into
 * {@link org.opengis.feature.simple.SimpleFeature}.
 *
 * @author Yu Liebing
 */
public class RowDataToSimpleFeatureConverter implements GeoMesaSimpleFeatureConverter<RowData> {

  private GeoMesaSQLTableSchema geomesaTableSchema;
  private transient SimpleFeatureBuilder builder;

  public RowDataToSimpleFeatureConverter(GeoMesaSQLTableSchema geomesaTableSchema) {
    this.geomesaTableSchema = geomesaTableSchema;
  }

  @Override
  public void open() {
    builder = new SimpleFeatureBuilder(geomesaTableSchema.getSimpleFeatureType());
  }

  @Override
  public SimpleFeature convertToSimpleFeature(RowData record) {
    for (int i = 0, len = record.getArity(); i < len; ++i) {
      setSimpleFeatureField(i, record);
    }
    return builder.buildFeature(getPrimaryKey(record));
  }

  @Override
  public String getPrimaryKey(RowData record) {
    int[] primaryKeyIndexes = geomesaTableSchema.getPrimaryFieldsIndexes();
    if (1 == primaryKeyIndexes.length) {
      return (String) geomesaTableSchema.getFieldEncoder(primaryKeyIndexes[0]).encode(record, primaryKeyIndexes[0]);
    } else {
      // 多个fields进行拼接
      StringBuilder builder = new StringBuilder();
      for (int primaryFieldsIndex : primaryKeyIndexes) {
        builder.append((String) geomesaTableSchema.getFieldEncoder(primaryFieldsIndex).encode(record, primaryFieldsIndex));
        builder.append("-");
      }
      return builder.subSequence(0, builder.length() - 1).toString();
    }
  }

  @Override
  public void setSimpleFeatureField(int i, RowData record) {
    builder.set(geomesaTableSchema.getFieldName(i), geomesaTableSchema.getFieldEncoder(i).encode(record, i));
  }
}
