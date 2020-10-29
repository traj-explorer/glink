package com.github.tm.glink.features.serialization;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author Yu Liebing
 * */
public class FlinkTrajectoryDeSerialize implements DeserializationSchema<TrajectoryPoint> {

  private String attributesSchema;
  private transient AvroTrajectoryPoint  avroTrajectoryPoint;

  public FlinkTrajectoryDeSerialize() {
    avroTrajectoryPoint = new AvroTrajectoryPoint();
  }

  public FlinkTrajectoryDeSerialize(String attributesSchema) {
    this.attributesSchema = attributesSchema;
    avroTrajectoryPoint = new AvroTrajectoryPoint(attributesSchema);
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
