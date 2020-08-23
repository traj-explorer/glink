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
public class KafkaProduceJob {

  public static String[] listFiles(String path) {
    File file = new File(path);
    return file.list();
  }

  public static void main(String[] args) throws FileNotFoundException, InterruptedException {
    boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");

    String path = KafkaProduceJob.class.getResource("/kafka").getPath();
    String[] files = listFiles(path);
    int threadNum = files.length;
    CountDownLatch latch = new CountDownLatch(threadNum);

    for (String file : files) {
      CSVPointProducer producer = new CSVPointProducer(
              path + File.separator + file,
              "localhost",
              9092,
              "point",
              "CSVPointProducer",
              StringSerializer.class.getName(),
              ByteArraySerializer.class.getName(),
              isAsync,
              latch);
      producer.start();
    }
    latch.await();
  }
}
