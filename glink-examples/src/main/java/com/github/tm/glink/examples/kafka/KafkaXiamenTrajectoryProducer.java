package com.github.tm.glink.examples.kafka;

import com.github.tm.glink.examples.utils.CSVXiamenTrajectoryProducer;
import com.github.tm.glink.examples.utils.CommonUtils;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 * */
public class KafkaXiamenTrajectoryProducer {
  public static void main(String[] args) throws FileNotFoundException, InterruptedException {
    String path = args[0];
    String topic = args[1];
    String brokerList = args[2];
    boolean isAsync = args.length == 4 && args[3].trim().equalsIgnoreCase("sync");

    String[] files = CommonUtils.listFiles(path);
    int threadNum = CommonUtils.getThreadNum(files.length);
    CountDownLatch latch = new CountDownLatch(threadNum);
    int i = 0;
    for (String file : files) {
      CSVXiamenTrajectoryProducer producer = new CSVXiamenTrajectoryProducer(
              file,
              brokerList,
              topic,
              "CSVXiamenTrajectoryProducer-" + i,
              StringSerializer.class.getName(),
              ByteArraySerializer.class.getName(),
              isAsync,
              latch);
      producer.start();
      ++i;
    }
    latch.await();
  }
}
