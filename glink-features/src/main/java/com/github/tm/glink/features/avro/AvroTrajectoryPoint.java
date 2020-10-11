package com.github.tm.glink.features.avro;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.avro.generic.GenericRecord;

import java.util.Properties;

/**
 * @author Yu Liebing
 * */
public class AvroTrajectoryPoint extends AvroGeoObject<TrajectoryPoint> {

  public static final String SCHEMA = "{" +
          "\"type\": \"record\"," +
          "\"name\": \"Point\"," +
          "\"fields\": [" +
              "{\"name\": \"id\", \"type\": \"string\"}," +
              "{\"name\": \"pid\", \"type\": \"int\"}," +
              "{\"name\": \"lat\", \"type\": \"double\"}, " +
              "{\"name\": \"lng\", \"type\": \"double\"}," +
              "{\"name\": \"timestamp\", \"type\": \"long\"}," +
              "{\"name\": \"index\", \"type\": \"long\"}" +
              "%s" + // attributes schema
          "]" +
          "}";

  public AvroTrajectoryPoint() {
    String schema = String.format(SCHEMA, "");
    init(schema);
  }

  public AvroTrajectoryPoint(String attributesSchema) {
    String schema = toAvroSchema(SCHEMA, attributesSchema);
    init(schema);
  }

  @Override
  public byte[] serialize(TrajectoryPoint trajectoryPoint) {
    genericRecord.put("id", trajectoryPoint.getId());
    genericRecord.put("pid", trajectoryPoint.getPid());
    genericRecord.put("lat", trajectoryPoint.getLat());
    genericRecord.put("lng", trajectoryPoint.getLng());
    genericRecord.put("timestamp", trajectoryPoint.getTimestamp());
    genericRecord.put("index", trajectoryPoint.getIndex());
    if (trajectoryPoint.getAttributes() != null) {
      serializeAttributes(genericRecord, trajectoryPoint.getAttributes());
    }
    return injection.apply(genericRecord);
  }

  @Override
  public TrajectoryPoint deserialize(byte[] data) {
    GenericRecord record = injection.invert(data).get();
    Properties attributes = deserializeAttributes(record);
    Point point = genericToTrajectoryPoint(record);
    point.setAttributes(attributes);
    return genericToTrajectoryPoint(record);
  }

  public static TrajectoryPoint genericToTrajectoryPoint(GenericRecord record) {
    String id = record.get("id").toString();
    int pid = (int) record.get("pid");
    double lat = (double) record.get("lat");
    double lng = (double) record.get("lng");
    long timestamp = (long) record.get("timestamp");
    long index = (long) record.get("index");
    return new TrajectoryPoint(id, pid, lat, lng, timestamp, index);
  }
}
