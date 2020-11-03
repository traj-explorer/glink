package com.github.tm.glink.features.serialization;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author Yu Liebing
 * */
public class FlinkTrajectoryPointSchema
        implements SerializationSchema<TrajectoryPoint>, DeserializationSchema<TrajectoryPoint> {

  private String attributesSchema;
  private transient AvroTrajectoryPoint  avroTrajectoryPoint;

  public FlinkTrajectoryPointSchema() {
    avroTrajectoryPoint = new AvroTrajectoryPoint();
  }

  public FlinkTrajectoryPointSchema(String attributesSchema) {
    this.attributesSchema = attributesSchema;
    avroTrajectoryPoint = new AvroTrajectoryPoint(attributesSchema);
  }

  @Override
  public byte[] serialize(TrajectoryPoint trajectoryPoint) {
    checkAvroInitialized();
    return avroTrajectoryPoint.serialize(trajectoryPoint);
  }

  @Override
  public TrajectoryPoint deserialize(byte[] bytes) throws IOException {
    checkAvroInitialized();
    return avroTrajectoryPoint.deserialize(bytes);
  }

  @Override
  public boolean isEndOfStream(TrajectoryPoint trajectoryPoint) {
    return false;
  }

  @Override
  public TypeInformation<TrajectoryPoint> getProducedType() {
    return TypeInformation.of(new TypeHint<TrajectoryPoint>() { });
  }

  private void checkAvroInitialized() {
    if (avroTrajectoryPoint == null) {
      if (attributesSchema == null) {
        avroTrajectoryPoint = new AvroTrajectoryPoint();
      } else {
        avroTrajectoryPoint = new AvroTrajectoryPoint(attributesSchema);
      }
    }
  }
}
