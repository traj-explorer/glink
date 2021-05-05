package com.github.tm.glink.connector.geomesa.util;

import com.github.tm.glink.connector.geomesa.options.GeoMesaConfigOption;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.locationtech.geomesa.utils.geotools.SchemaBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tm.glink.connector.geomesa.util.GeoMesaSerde.GeoMesaFieldDecoder;
import static com.github.tm.glink.connector.geomesa.util.GeoMesaSerde.GeoMesaFieldEncoder;

/**
 * Helps to specify a Geomesa Table's schema.
 *
 * @author Yu Liebing
 * */
public final class GeoMesaTableSchema implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final TemporalJoinPredict DEFAULT_TEMPORAL_JOIN_PREDICT = TemporalJoinPredict.INTERSECTS;

  private String schemaName;
  private String defaultGeometry;
  private String defaultDate;
  private String indicesInfo;
  // join parameters, when do temporal table join with geomesa
  private double joinDistance = 0.d;
  private TemporalJoinPredict temporalJoinPredict = DEFAULT_TEMPORAL_JOIN_PREDICT;
  private List<Tuple2<String, GeoMesaType>> fieldNameToType = new ArrayList<>();
  private List<GeoMesaFieldEncoder> fieldEncoders = new ArrayList<>();
  private List<GeoMesaFieldDecoder> fieldDecoders = new ArrayList<>();
  private Map<String, Serializable> indexedDateAttribute = new HashMap<>();
  private Map<String, GeoMesaType> spatialFields;
  private int defaultIndexedSpatialFieldIndex;  // 在table中的Index
  private Map<String, GeoMesaType> primaryFields;
  private int primaryKeyIndex; // 在table中的Index
  private int primaryKeyIndexInUserData = Integer.MAX_VALUE; // 在作为属性的UserData中的Index

  private GeoMesaTableSchema() { }

  public int getFieldNum() {
    return fieldNameToType.size();
  }

  public SimpleFeatureType getSchema() {
    SchemaBuilder builder  = SchemaBuilder.builder();
    for (Tuple2<String, GeoMesaType> ft : fieldNameToType) {
      boolean isDefault = ft.f0.equals(defaultGeometry) || ft.f0.equals(defaultDate);
      switch (ft.f1) {
        case POINT:
          builder.addPoint(ft.f0, isDefault);
          break;
        case DATE:
        case TIMESTAMP:
          builder.addDate(ft.f0, isDefault);
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
        case BYTES:
          builder.addBytes(ft.f0);
          break;
        case BOOLEAN:
          builder.addBoolean(ft.f0);
          break;
        case INTEGER:
          builder.addInt(ft.f0);
          break;
        case POLYGON:
          builder.addPolygon(ft.f0, isDefault);
          break;
        case GEOMETRY:
          break;
        case LINE_STRING:
          builder.addLineString(ft.f0, isDefault);
          break;
        case MULTI_POINT:
          builder.addMultiPoint(ft.f0, isDefault);
          break;
        case MULTI_POLYGON:
          builder.addMultiPolygon(ft.f0, isDefault);
          break;
        case MULTI_LINE_STRING:
          builder.addMultiLineString(ft.f0, isDefault);
          break;
        case GEOMETRY_COLLECTION:
          builder.addGeometryCollection(ft.f0, isDefault);
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + ft.f1);
      }
    }
    SimpleFeatureType sft = builder.build(schemaName);
    // add indexed dated attribute
    for (Map.Entry<String, Serializable> e : indexedDateAttribute.entrySet()) {
      sft.getUserData().put(e.getKey(), e.getValue());
    }
    return sft;
  }

  public String getIndicesInfo () { return indicesInfo;}

  public GeoMesaFieldEncoder getFieldEncoder(int pos) {
    return fieldEncoders.get(pos);
  }

  public GeoMesaFieldDecoder getFieldDecoder(int pos) {
    return fieldDecoders.get(pos);
  }

  public String getPrimaryKey(RowData record) {
    return (String) fieldEncoders.get(primaryKeyIndex).encode(record, primaryKeyIndex);
  }

  public Integer getPrimaryKeyIndexInUserData() {
    if (primaryKeyIndexInUserData == Integer.MAX_VALUE) {
      if (primaryKeyIndexInUserData < defaultIndexedSpatialFieldIndex) {
        primaryKeyIndexInUserData =  primaryKeyIndex;
      } else {
        primaryKeyIndexInUserData =  primaryKeyIndex - 1;
      }
    }
    return primaryKeyIndexInUserData;
  }

  public String getFieldName(int pos) {
    return fieldNameToType.get(pos).f0;
  }

  public TemporalJoinPredict getTemporalJoinPredict() {
    return temporalJoinPredict;
  }

  public double getJoinDistance() {
    return joinDistance;
  }

  public static GeoMesaTableSchema fromTableSchemaAndOptions(TableSchema tableSchema, ReadableConfig readableConfig) {
    GeoMesaTableSchema geomesaTableSchema = new GeoMesaTableSchema();
    // schema name
    geomesaTableSchema.schemaName = readableConfig.get(GeoMesaConfigOption.GEOMESA_SCHEMA_NAME);
    // primary key name
    // only supports 1 field as primary key
    String primaryKey = tableSchema.getPrimaryKey().get().getColumns().get(0);
    // spatial fields
    geomesaTableSchema.spatialFields = getSpatialFields(readableConfig.get(GeoMesaConfigOption.GEOMESA_SPATIAL_FIELDS));
    // indices;
    geomesaTableSchema.indicesInfo = readableConfig.get(GeoMesaConfigOption.GEOMESA_INDICES_ENABLED);
    // all fields and field encoders
    String[] fieldNames = tableSchema.getFieldNames();
    DataType[] fieldTypes = tableSchema.getFieldDataTypes();
    // default geometry and Data field
    geomesaTableSchema.defaultGeometry = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_GEOMETRY);
    geomesaTableSchema.defaultDate = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_DATE);
    checkDefaultIndexFields(geomesaTableSchema.defaultGeometry, geomesaTableSchema.defaultDate, fieldNames);
    for (int i = 0; i < fieldNames.length; ++i) {
      // check primary key
      if (primaryKey.equals(fieldNames[i])) {
        GeoMesaType primaryKeyType = GeoMesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
        if (primaryKeyType != GeoMesaType.STRING) {
          throw new IllegalArgumentException("Geomesa only supports STRING primary key.");
        }
        geomesaTableSchema.primaryKeyIndex = i;
      }
      boolean isSpatialField = geomesaTableSchema.spatialFields.containsKey(fieldNames[i]);
      Tuple2<String, GeoMesaType> ft = new Tuple2<>();
      ft.f0 = fieldNames[i];
      if (isSpatialField) {
        ft.f1 = geomesaTableSchema.spatialFields.get(fieldNames[i]);
      } else {
        ft.f1 = GeoMesaType.mapLogicalTypeToGeomesaType(fieldTypes[i].getLogicalType());
      }
      geomesaTableSchema.fieldNameToType.add(ft);
      geomesaTableSchema.fieldEncoders.add(
              GeoMesaSerde.getGeoMesaFieldEncoder(fieldTypes[i].getLogicalType(), isSpatialField));
      geomesaTableSchema.fieldDecoders.add(
              GeoMesaSerde.getGeoMesaFieldDecoder(fieldTypes[i].getLogicalType(), isSpatialField));
    }
    // temporal table join parameters
    String joinPredict = readableConfig.get(GeoMesaConfigOption.GEOMESA_TEMPORAL_JOIN_PREDICT);
    geomesaTableSchema.validTemporalJoinParam(joinPredict);
    return geomesaTableSchema;
  }

  public static GeoMesaTableSchema fromFieldNamesAndTypes(List<Tuple2<String, GeoMesaType>> fieldNamesToTypes, ReadableConfig readableConfig) {
    GeoMesaTableSchema geomesaTableSchema = new GeoMesaTableSchema();
    geomesaTableSchema.fieldNameToType = fieldNamesToTypes;
    // set pk index and type check
    String primaryFieldName = readableConfig.get(GeoMesaConfigOption.PRIMARY_FIELD_NAME);
    int temp = 0;
    String[] fieldNames = new String[fieldNamesToTypes.size()];
    for (Tuple2 tup : fieldNamesToTypes) {
      if (tup.f0 == primaryFieldName) {
        if (tup.f1 != GeoMesaType.STRING) {
          throw new IllegalArgumentException("Geomesa only supports STRING primary key.");
        }
        geomesaTableSchema.primaryKeyIndex = temp;
      }
      fieldNames[temp] = (String)tup.f0;
      temp ++;
    }
    // schema name
    geomesaTableSchema.schemaName = readableConfig.get(GeoMesaConfigOption.GEOMESA_SCHEMA_NAME);
    // spatial fields
    geomesaTableSchema.spatialFields = getSpatialFields(readableConfig.get(GeoMesaConfigOption.GEOMESA_SPATIAL_FIELDS));
    // TODO: 如果存在多个spatial fields...
    geomesaTableSchema.setDefaultSpatialFieldIndex();
    // indices
    geomesaTableSchema.indicesInfo = readableConfig.get(GeoMesaConfigOption.GEOMESA_INDICES_ENABLED);
    // default geometry and Data field
    geomesaTableSchema.defaultGeometry = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_GEOMETRY);
    geomesaTableSchema.defaultDate = readableConfig.get(GeoMesaConfigOption.GEOMESA_DEFAULT_DATE);
    checkDefaultIndexFields(geomesaTableSchema.defaultGeometry, geomesaTableSchema.defaultDate, fieldNames);

    // temporal table join parameters
    String joinPredict = readableConfig.get(GeoMesaConfigOption.GEOMESA_TEMPORAL_JOIN_PREDICT);
    geomesaTableSchema.validTemporalJoinParam(joinPredict);

    return geomesaTableSchema;
  }

  // 可能有多个spatial fields
  private static Map<String, GeoMesaType> getSpatialFields(String spatialFields) {
    Map<String, GeoMesaType> nameToType = new HashMap<>();
    if (spatialFields == null) return nameToType;
    String[] items = spatialFields.split(",");
    for (String item : items) {
      String[] nt = item.split(":");
      String name = nt[0];
      GeoMesaType type = GeoMesaType.getGeomesaType(nt[1]);
      nameToType.put(name, type);
    }
    return nameToType;
  }

  // init the first spatial field index
  private void setDefaultSpatialFieldIndex() {
    for (int i = 0; i < fieldNameToType.size(); i++) {
      if (getSpatialFieldNames()[0].equalsIgnoreCase(fieldNameToType.get(i).f0)) {
        this.defaultIndexedSpatialFieldIndex = i;
        return;
      } else {
        i++;
      }
    }
    this.defaultIndexedSpatialFieldIndex = -1;
  }


  public String[] getSpatialFieldNames() {
    String[] res = new String[spatialFields.size()];
    int i = 0;
    for (String key : spatialFields.keySet()){
      res[i] = key;
    }
    return res;
  }

  // 获取默认的（第一个）几何字段的index
  public int getDefaultIndexedSpatialFieldIndex() {
    return defaultIndexedSpatialFieldIndex;
  }

  private static void checkDefaultIndexFields(String defaultGeometry, String defaultDate, String[] fieldNames) {
    if (defaultGeometry == null && defaultDate == null) return;
    boolean isGeometryValid = false, isDateValid = false;
    for (String fieldName : fieldNames) {
      if (fieldName.equals(defaultGeometry)) isGeometryValid = true;
      if (fieldName.equals(defaultDate)) isDateValid = true;
    }
    if (defaultGeometry != null && !isGeometryValid)
      throw new IllegalArgumentException("The default geometry field is not in the table schema.");
    if (defaultDate != null && !isDateValid)
      throw new IllegalArgumentException("The default Date field is not in the table schema.");
  }

  private void validTemporalJoinParam(String joinPredict) {
    if (joinPredict == null) return;
    if (joinPredict.startsWith("R")) {
      String[] items = joinPredict.split(":");
      if (items.length != 2) {
        throw new IllegalArgumentException("geomesa.temporal.join.predict support R:<distance> format for distance join");
      }
      temporalJoinPredict = TemporalJoinPredict.RADIUS;
      joinDistance = Double.parseDouble(items[1]);
    } else {
      temporalJoinPredict = TemporalJoinPredict.getTemporalJoinPredict(joinPredict);
    }
  }
}
