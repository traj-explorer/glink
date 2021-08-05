package com.github.tm.glink.examples.demo.nyc;

import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.examples.demo.xiamen.CSVStringSourceSimulation;

import java.io.IOException;
import java.util.Properties;

import static com.github.tm.glink.examples.demo.nyc.Heatmap.KAFKA_BOOSTRAP_SERVERS;

/**
 * @author Wang Haocheng
 * @date 2021/6/16 - 4:54 下午
 */
public class KafkaDataProducer {
  public static final String FILEPATH = "/mnt/hgfs/disk/data/nyc-yellow/NYC2009.csv";
  public static final String TOPICID = "NYCdata";
  public static final int SPEED_UP = 1200;
  public static final int TIMEFIELDINDEX = 0;
  public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA_BOOSTRAP_SERVERS);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    CSVStringSourceSimulation simulation = new CSVStringSourceSimulation(props, TOPICID, FILEPATH, SPEED_UP, TIMEFIELDINDEX, SPLITTER, false);
    simulation.run();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        simulation.close();
      } catch (IOException e) {
        System.err.println("******** Close source Failed! ********");
        e.printStackTrace();
      }
      System.out.println("******** Kafka source closed! ********");
    }));
  }
}
