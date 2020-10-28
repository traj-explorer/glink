package com.github.tm.glink.examples.mapmatching;

import com.github.tm.glink.examples.utils.CommonUtils;
import com.github.tm.glink.examples.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 */
public class MapMatchingProducer {

  /**
   * @param args 0 (string) - dir contains trajectory files
   *             1 (string) - kafka topic
   *             2 (string) - kafka broker list
   *             3 (int) - num partitions
   *             4 (int) - sleep seconds: -1 for throughput test
   *             5 (string, option) - `sync` to indicate kafka send message synchronized
   * */
  public static void main(String[] args) throws Exception {
    String path = args[0];
    String topic = args[1];
    String brokerList = args[2];
    int numPartitions = Integer.parseInt(args[3]);
    int sleep = Integer.parseInt(args[4]);
    boolean isAsync = args.length == 6 && args[5].trim().equalsIgnoreCase("sync");

    Properties properties = new Properties();
    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    if (KafkaUtils.isTopicExists(topic, properties)) {
      System.out.println("topic already exists, deleting");
      KafkaUtils.deleteTopic(topic, properties);
      System.out.println("create new topic");
      KafkaUtils.createTopic(topic, numPartitions, 1, properties, null);
    }

    String[] files = CommonUtils.listFiles(path);
    if (files == null) {
      System.out.printf("No files in dir %s\n", path);
      System.exit(1);
    }
    int threadNum = CommonUtils.getThreadNum(files.length);
    List<List<String>> fileDistributions = CommonUtils.distributionFiles(files, threadNum);
    CountDownLatch latch = new CountDownLatch(threadNum);
    int i = 0;
    for (List<String> threadFiles : fileDistributions) {
      XiamenTrajectoryCSVProducer producer = new XiamenTrajectoryCSVProducer(
              threadFiles,
              brokerList,
              topic,
              "XiamenTrajectoryCSVProducer" + i,
              StringSerializer.class.getName(),
              ByteArraySerializer.class.getName(),
              isAsync,
              latch,
              sleep);
      producer.start();
      ++i;
    }
    latch.await();
  }
}
