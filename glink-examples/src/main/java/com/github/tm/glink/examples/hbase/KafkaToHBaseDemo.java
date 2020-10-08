package com.github.tm.glink.examples.hbase;

import com.github.tm.glink.features.avro.AvroPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import com.github.tm.glink.hbase.sink.HBaseTableSink;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * This class import simple point data from kafka broker to hbase table
 *
 * @author Yu Liebing
 * */
public class KafkaToHBaseDemo {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // kafka deserialize schema
    Schema schema = new Schema.Parser().parse(AvroPoint.SCHEMA);
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<GenericRecord> consumer = new FlinkKafkaConsumer<>(
            "hbase-demo", AvroDeserializationSchema.forGeneric(schema), props);
    DataStream<GenericRecord> dataStream = env.addSource(consumer);
    dataStream.map(AvroPoint::genericToPoint)
            .addSink(new HBaseTableSink<>("point"));

    dataStream.print();

    env.execute();
  }
}
