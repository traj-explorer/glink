package com.github.tm.glink.connector.geomesa.util;

import com.github.tm.glink.connector.geomesa.options.GeomesaConfigOption;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.locationtech.geomesa.utils.geotools.SchemaBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;
import java.util.*;

import static com.github.tm.glink.connector.geomesa.util.GeomesaSerde.*;

/**
 * Helps to specify a Geomesa Table's schema.
 * */
public class GeomesaTableSchema implements Serializable {

  private static final long serialVersionUID = 1L;

  private String schemaName;
  private int primaryKeyIndex;
  private List<Tuple2<String, GeomesaType>> fieldNameToType = new ArrayList<>();
  private List<GeomesaFieldEncoder> fieldEncoders = new ArrayList<>();

  private Map<String, Serializable> indexedDateAttribute = new HashMap<>();

  private GeomesaTableSchema() { }

  public SimpleFeatureType getSchema() {
    SchemaBuilder builder  = SchemaBuilder.builder();
    for (Tuple2<String, GeomesaType> ft : fieldNameToType) {
      switch (ft.f1) {
        case POINT:
          builder.addPoint(ft.f0, false);
          break;
        case DATE:
        case TIMESTAMP:
          builder.addDate(ft.f0, false);
          break;
        case LONG:
          builder.addLong(ft.f0);
          break;
        case UUID:
          builder.addUuid(ft.f0);
          break;
        case FLOAT:
          builder.addFloat(ft.f0);
          break;
        case DOUBLE:
          builder.addDouble(ft.f0);
          break;
        case STRING:
          builder.addString(ft.f0);
          break;
        case BOOLEAN:
          builder.addBoolean(ft.f0);
          break;
        case INTEGER:
          builder.addInt(ft.f0);
          break;
        case POLYGON:
          builder.addPolygon(ft.f0, false);
          break;
        case GEOMETRY:
          break;
        case LINE_STRING:
          builder.addLineString(ft.f0, false);
          break;
        case MULTI_POINT:
          builder.addMultiPoint(ft.f0, false);
          break;
        case MULTI_POLYGON:
          builder.addMultiPolygon(ft.f0, false);
          break;
        case MULTI_LINE_STRING:
          builder.addMultiLineString(ft.f0,false);
          break;
        case GEOMETRY_COLLECTION:
          builder.addGeometryCollection(ft.f0, false);
          break;
      }
    }
    SimpleFeatureType sft = builder.build(schemaName);
    // add indexed dated attribute
    for (Map.Entry<String, Serializable> e : indexedDateAttribute.entrySet()) {
      sft.getUserData().put(e.getKey(), e.getValue());
    }
    return sft;
  }

  public GeomesaFieldEncoder getFieldEncoder(int pos) {
    return fieldEncoders.get(pos);
  }

  public String getPrimaryKey(RowData record) {
    return (String) fieldEncoders.get(primaryKeyIndex).encode(record, primaryKeyIndex);
  }

  public String getFieldName(int pos) {
    return fieldNameToType.get(pos).f0;
  }

  public static GeomesaTableSchema fromTableSchemaAndOptions(TableSchema tableSchema, ReadableConfig readableConfig) {
    GeomesaTableSchema geomesaTableSchema = new GeomesaTableSchema();
    // schema name
    geomesaTableSchema.schemaName = readableConfig.get(GeomesaConfigOption.GEOMESA_SCHEMA_NAME);
    // primary key name
    String primaryKey = tableSchema.getPrimaryKey().get().getColumns().get(0);
    // spatial fields
    Map<String, GeomesaType> spatialFields = getSpatialFields(
            readableConfig.get(GeomesaConfigOption.GEOMESA_SPATIAL_FIELDS));
    // all fields and field encoders
    String[] fieldNames = tableSchema.getFieldNames();
    DataType[] fieldTypes = tableSchema.getFieldDataTypes();
    for (int i = 0; i < fieldNames.length; ++i) {
      // check primary key
      if (primaryKey.equals(fieldNames[i])) {
        GeomesaType primaryKeyType = GeomesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
        if (primaryKeyType != GeomesaType.STRING) {
          throw new IllegalArgumentException("Geomesa only supports STRING primary key.");
        }
        geomesaTableSchema.primaryKeyIndex = i;
      }
      boolean isSpatialField = spatialFields.containsKey(fieldNames[i]);
      Tuple2<String, GeomesaType> ft = new Tuple2<>();
      ft.f0 = fieldNames[i];
      if (isSpatialField) {
        ft.f1 = spatialFields.get(fieldNames[i]);
      } else {
        ft.f1 = GeomesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
      }
      geomesaTableSchema.fieldNameToType.add(ft);
      geomesaTableSchema.fieldEncoders.add(
              GeomesaSerde.getGeomesaFieldEncoder(fieldTypes[i].getLogicalType(), isSpatialField));
    }
    return geomesaTableSchema;
  }

  private static Map<String, GeomesaType> getSpatialFields(String spatialFields) {
    if (spatialFields == null) return null;
    Map<String, GeomesaType> nameToType = new HashMap<>();
    String[] items = spatialFields.split(",");
    for (String item : items) {
      String[] nt = item.split(":");
      String name = nt[0];
      GeomesaType type = GeomesaType.getGeomesaType(nt[1]);
      nameToType.put(name, type);
    }
    return nameToType;
  }
}
