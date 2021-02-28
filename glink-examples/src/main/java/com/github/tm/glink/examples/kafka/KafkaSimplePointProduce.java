package com.github.tm.glink.examples.kafka;

import com.github.tm.glink.kafka.CSVPointProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 * */
public class KafkaSimplePointProduce {

  private static int getThreadNum(int fileNum) {
    int maxThreads = Runtime.getRuntime().availableProcessors() + 1;
    return Math.min(fileNum, maxThreads);
  }

  private static String[] listFiles(String path) {
    File file = new File(path);
    return file.list();
  }

  public static void main(String[] args) throws FileNotFoundException, InterruptedException {
    String path = args[0];
    String topic = args[1];
    String brokerList = args[2];
    boolean isAsync = args.length == 4 && args[3].trim().equalsIgnoreCase("sync");

    String[] files = listFiles(path);
    int threadNum = getThreadNum(files.length);
    CountDownLatch latch = new CountDownLatch(threadNum);
    int i = 0;
    for (String file : files) {
      CSVPointProducer producer = new CSVPointProducer(
              path + File.separator + file,
              brokerList,
              topic,
              "CSVPointProducer-" + i,
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
