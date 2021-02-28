package com.github.tm.glink.features.serialization;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FlinkKafkaSerializeSchema implements KafkaSerializationSchema<TrajectoryPoint> {

  private String topic;
  private String attributesSchema;
  private transient AvroTrajectoryPoint avroTrajectoryPoint;

  public FlinkKafkaSerializeSchema(String topic) {
    this(topic, null);
  }

  public FlinkKafkaSerializeSchema(String topic, String attributesSchema) {
    this.topic = topic;
    this.attributesSchema = attributesSchema;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(TrajectoryPoint trajectoryPoint, Long timestamp) {
    checkAvroInitialized();
    return new ProducerRecord<>(
            topic, avroTrajectoryPoint.serialize(trajectoryPoint));
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
