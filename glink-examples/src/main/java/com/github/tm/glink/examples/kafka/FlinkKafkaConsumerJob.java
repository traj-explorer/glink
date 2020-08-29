package com.github.tm.glink.examples.kafka;

import com.github.tm.glink.features.avro.AvroPoint;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class FlinkKafkaConsumerJob {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Schema schema = new Schema.Parser().parse(AvroPoint.SCHEMA);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<GenericRecord> consumer = new FlinkKafkaConsumer<>(
            "point", AvroDeserializationSchema.forGeneric(schema), props);
    DataStream<GenericRecord> dataStream = env.addSource(consumer);

    dataStream.print();

    env.execute();
  }
}
