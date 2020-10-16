package com.github.tm.glink.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ThroughputPerformance {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId19");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);

    consumer.subscribe(Collections.singletonList("XiamenTrajectory"));
    long timeWindow = 100;
    long startTime = 0;
    long endTime = 0;
    long realEndTime = 0;

    int windowNum = 0;
    while (true) {
      ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
      int num = 0;
      for (ConsumerRecord<Integer, String> record : records) {
        if (startTime == 0) {
          startTime = record.timestamp();
          endTime = startTime + timeWindow;
        }
        if (record.timestamp() >= endTime) {
          startTime = record.timestamp();
          endTime = startTime + timeWindow;
          double throughput = windowNum / ((double) timeWindow / 1000);
          System.out.println(throughput);
          windowNum = 0;
        }
        realEndTime = record.timestamp();
        ++windowNum;
        ++num;
      }

      if (num == 0) {
        double timeInterval = ((double) (realEndTime - startTime)) / 1000;
//        System.out.println(windowNum);
//        System.out.println(timeInterval);
        double throughput = windowNum / timeInterval;
        System.out.println(throughput);
        break;
      }
    }
  }
}
