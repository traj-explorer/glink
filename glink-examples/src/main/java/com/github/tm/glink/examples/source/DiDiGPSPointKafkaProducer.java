package com.github.tm.glink.examples.source;
import com.github.tm.glink.source.DidiGPSPointToKafkaSender;
import org.apache.avro.Schema;
import java.io.IOException;
import java.util.Properties;

public class DiDiGPSPointKafkaProducer {

  private static final int SERVING_SPEED = 1;
  private static final long DATA_START_TIME = 1477955400L;
  private static final String TOPIC_NAME = "DidiPoint-topic001";



  public static void main(String[] args) throws IOException, InterruptedException {
    Properties kafkaProps = new Properties();
    String absoluteFilePath = "/Users/haocheng/Desktop/glink/glink-examples/src/main/resources/gps_20161101_0710";
    kafkaProps.put("bootstrap.servers", "localhost:9092");
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
    // 这里使用的是Confluent提供的 Kafka Schema Registry。
    kafkaProps.put("schema.registry.url", "http://localhost:8081");
    DidiGPSPointToKafkaSender sender = new DidiGPSPointToKafkaSender(kafkaProps, TOPIC_NAME, absoluteFilePath, SERVING_SPEED, DATA_START_TIME);
    sender.generateSource();
  }
}
