package com.github.tm.glink.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author Yu Liebing
 */
public class ThroughputPerformance {

  public static void main(String[] args) {
    String brokerList = args[0];
    String groupId = args[1];
    String topic = args[2];
    long timeWindow = Long.parseLong(args[3]) * 1000;

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);

    consumer.subscribe(Collections.singletonList(topic));
    long startTime = 0;
    long endTime = 0;
    long realEndTime = 0;

    List<Double> throughputList = new ArrayList<>();
    int windowNum = 0;
    int totalNum = 0;
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
          throughputList.add(throughput);
          System.out.println("window num: " + windowNum);
          windowNum = 0;
        }
        realEndTime = record.timestamp();
        ++windowNum;
        ++num;
        ++totalNum;
      }

      if (num == 0) {
//        double timeInterval = ((double) (realEndTime - startTime)) / 1000;
//        double throughput = windowNum / timeInterval;
//        System.out.println("final time interval: " + timeInterval);
//        System.out.println("final windowNum: " + windowNum);
//        throughputList.add(throughput);
        break;
      }
    }
    System.out.println("total num: " + totalNum);
    double totalThroughput = 0.d;
    for (double t : throughputList) {
      totalThroughput += t;
      System.out.println(t);
    }
    double avgThroughput = totalThroughput / throughputList.size();
    System.out.printf("avg throughput: %f\n", avgThroughput);
  }
}
