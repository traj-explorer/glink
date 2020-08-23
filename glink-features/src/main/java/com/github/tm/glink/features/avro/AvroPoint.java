package com.github.tm.glink.features.avro;

import com.github.tm.glink.features.Point;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * @author Yu Liebing
 * */
public class AvroPoint extends AvroGeoObject<Point> {

  public static final String SCHEMA = "{" +
          "\"type\": \"record\"," +
          "\"name\": \"Point\"," +
          "\"fields\": [" +
              "{\"name\": \"id\", \"type\": \"string\"}," +
              "{\"name\": \"lat\", \"type\": \"double\"}, " +
              "{\"name\": \"lng\", \"type\": \"double\"}," +
              "{\"name\": \"timestamp\", \"type\": \"long\"}," +
              "{\"name\": \"index\", \"type\": \"long\"}" +
            "]" +
          "}";
  private final Schema schema = new Schema.Parser().parse(SCHEMA);
  private final GenericRecord genericRecord = new GenericData.Record(schema);
  private final Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);

  @Override
  public byte[] serialize(Point point) {
    genericRecord.put("id", point.getId());
    genericRecord.put("lat", point.getLat());
    genericRecord.put("lng", point.getLng());
    genericRecord.put("timestamp", point.getTimestamp());
    genericRecord.put("index", point.getIndex());
    return injection.apply(genericRecord);
  }

  @Override
  public Point deserialize(byte[] data) {
    GenericRecord record = injection.invert(data).get();
    String id = record.get("id").toString();
    double lat = (double) record.get("lat");
    double lng = (double) record.get("lng");
    long timestamp = (long) record.get("timestamp");
    long index = (long) record.get("index");
    return new Point(id, lat, lng, timestamp, index);
  }
}
