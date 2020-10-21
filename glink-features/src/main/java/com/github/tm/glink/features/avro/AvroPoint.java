package com.github.tm.glink.features.avro;

import com.github.tm.glink.features.Point;
import org.apache.avro.generic.GenericRecord;

import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class AvroPoint extends AvroGeoObject<Point> {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static final String SCHEMA = "{" +
          "\"type\": \"record\"," +
          "\"name\": \"Point\"," +
          "\"fields\": [" +
              "{\"name\": \"id\", \"type\": [\"string\", \"null\"]}," +
              "{\"name\": \"lat\", \"type\": \"double\"}, " +
              "{\"name\": \"lng\", \"type\": \"double\"}," +
              "{\"name\": \"timestamp\", \"type\": [\"long\", \"null\"]}," +
              "{\"name\": \"index\", \"type\": [\"long\", \"null\"]}" +
              "%s" + // attributes schema
          "]" +
          "}";

  public AvroPoint() {
    String schema = String.format(SCHEMA, "");
    init(schema);
  }

  public AvroPoint(String attributesSchema) {
    String schema = toAvroSchema(SCHEMA, attributesSchema);
    init(schema);
  }

  @Override
  public byte[] serialize(Point point) {
    genericRecord.put("id", point.getId());
    genericRecord.put("lat", point.getLat());
    genericRecord.put("lng", point.getLng());
    genericRecord.put("timestamp", point.getTimestamp());
    genericRecord.put("index", point.getIndex());
    if (point.getAttributes() != null) {
      serializeAttributes(genericRecord, point.getAttributes());
    }
    return injection.apply(genericRecord);
  }

  @Override
  public Point deserialize(byte[] data) {
    GenericRecord record = injection.invert(data).get();
    Properties attributes = deserializeAttributes(record);
    Point point = genericToPoint(record);
    point.setAttributes(attributes);
    return point;
  }

  public static Point genericToPoint(GenericRecord record) {
    String id = record.get("id").toString();
    double lat = (double) record.get("lat");
    double lng = (double) record.get("lng");
    long timestamp = (long) record.get("timestamp");
    long index = (long) record.get("index");
    return new Point(id, lat, lng, timestamp, index);
  }
}
