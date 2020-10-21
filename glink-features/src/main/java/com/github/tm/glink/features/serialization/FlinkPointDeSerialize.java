package com.github.tm.glink.features.serialization;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.avro.AvroPoint;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author Yu Liebing
 * */
public class FlinkPointDeSerialize implements DeserializationSchema<Point> {

  private String attributesSchema;
  private transient AvroPoint avroPoint;

  public FlinkPointDeSerialize() {
    avroPoint = new AvroPoint();
  }

  public FlinkPointDeSerialize(String attributesSchema) {
    this.attributesSchema = attributesSchema;
    avroPoint = new AvroPoint(attributesSchema);
  }

  @Override
  public Point deserialize(byte[] bytes) throws IOException {
    checkAvroInitialized();
    return avroPoint.deserialize(bytes);
  }

  @Override
  public boolean isEndOfStream(Point point) {
    return false;
  }

  @Override
  public TypeInformation<Point> getProducedType() {
    return TypeInformation.of(new TypeHint<Point>() { });
  }

  private void checkAvroInitialized() {
    if (avroPoint == null) {
      if (attributesSchema == null) {
        avroPoint = new AvroPoint();
      } else {
        avroPoint = new AvroPoint(attributesSchema);
      }
    }
  }
}
