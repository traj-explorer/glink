package com.github.tm.glink.connector.geomesa.sink;

import com.github.tm.glink.connector.geomesa.util.GeomesaTableSchema;
import org.apache.flink.table.data.RowData;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;

/**
 * An implementation of {@link GeomesaSimpleFeatureConverter} which converts {@link RowData} into
 * {@link org.opengis.feature.simple.SimpleFeature}.
 */
public class RowDataToSimpleFeatureConverter implements GeomesaSimpleFeatureConverter<RowData> {

  private GeomesaTableSchema geomesaTableSchema;
  private transient SimpleFeatureBuilder builder;
  private transient WKTReader wktReader;

  public RowDataToSimpleFeatureConverter(GeomesaTableSchema geomesaTableSchema) {
    this.geomesaTableSchema = geomesaTableSchema;
  }

  @Override
  public void open() {
    builder = new SimpleFeatureBuilder(geomesaTableSchema.getSchema());
    wktReader = new WKTReader();
  }

  @Override
  public SimpleFeature convertToSimpleFeature(RowData record) {
    for (int i = 0, len = record.getArity(); i < len; ++i) {
      builder.set(geomesaTableSchema.getFieldName(i), geomesaTableSchema.getFieldEncoder(i).encode(record, i));
    }
//    SimpleFeature sf = builder.buildFeature(geomesaTableSchema.getPrimaryKey(record));
    return builder.buildFeature(geomesaTableSchema.getPrimaryKey(record));
  }
}