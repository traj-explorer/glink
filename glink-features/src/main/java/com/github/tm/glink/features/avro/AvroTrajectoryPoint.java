package com.github.tm.glink.features.avro;

import com.github.tm.glink.features.TrajectoryPoint;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

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
            "]" +
          "}";
  private final Schema schema = new Schema.Parser().parse(SCHEMA);
  private final GenericRecord genericRecord = new GenericData.Record(schema);
  private final Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);

  @Override
  public byte[] serialize(TrajectoryPoint trajectoryPoint) {
    genericRecord.put("id", trajectoryPoint.getId());
    genericRecord.put("pid", trajectoryPoint.getPid());
    genericRecord.put("lat", trajectoryPoint.getLat());
    genericRecord.put("lng", trajectoryPoint.getLng());
    genericRecord.put("timestamp", trajectoryPoint.getTimestamp());
    genericRecord.put("index", trajectoryPoint.getIndex());
    return injection.apply(genericRecord);
  }

  @Override
  public TrajectoryPoint deserialize(byte[] data) {
    GenericRecord record = injection.invert(data).get();
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
