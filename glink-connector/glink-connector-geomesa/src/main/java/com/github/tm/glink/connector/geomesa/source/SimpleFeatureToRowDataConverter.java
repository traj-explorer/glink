package com.github.tm.glink.connector.geomesa.source;

import com.github.tm.glink.connector.geomesa.util.GeoMesaSQLTableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.opengis.feature.simple.SimpleFeature;

/**
 * An implementation of {@link GeoMesaGlinkObjectConverter} which converts {@link org.opengis.feature.simple.SimpleFeature} into
 * {@link RowData}.
 *
 * @author Yu Liebing
 */
public class SimpleFeatureToRowDataConverter implements GeoMesaGlinkObjectConverter<RowData> {

  private GeoMesaSQLTableSchema tableSchema;

  public SimpleFeatureToRowDataConverter(GeoMesaSQLTableSchema geoMesaTableSchema) {
    tableSchema = geoMesaTableSchema;
  }

  @Override
  public void open() { }

  @Override
  public RowData convertToFlinkObj(SimpleFeature sf) {
    final int fieldNum = tableSchema.getFieldNum();
    GenericRowData rowData = new GenericRowData(fieldNum);
    for (int i = 0; i < fieldNum; ++i) {
      setGlinkObjectField(i, rowData, sf);
    }
    return rowData;
  }

  @Override
  public RowData setGlinkObjectField(int offsetInSchema, RowData record, SimpleFeature sf) {
    ((GenericRowData) record).setField(offsetInSchema, tableSchema.getFieldValue(offsetInSchema, sf));
    return record;
  }
}
