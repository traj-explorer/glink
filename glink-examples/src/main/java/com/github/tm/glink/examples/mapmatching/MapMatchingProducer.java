package com.github.tm.glink.examples.mapmatching;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 */
public class MapMatchingProducer {

  /**
   * @param args 0 (string) - dir contains trajectory files
   *             1 (string) - kafka topic
   *             2 (string) - kafka host
   *             3 (int) - kafka port
   *             4 (string, option) - `sync` to indicate kafka send message synchronized
   * */
  public static void main(String[] args) throws FileNotFoundException, InterruptedException {
    String path = args[0];
    String topic = args[1];
    String host = args[2];
    int port = Integer.parseInt(args[3]);
    boolean isAsync = args.length == 5 && args[4].trim().equalsIgnoreCase("sync");

    String[] files = listFiles(path);
    int threadNum = getThreadNum(files.length);
    CountDownLatch latch = new CountDownLatch(threadNum);
    int i = 0;
    for (String file : files) {
      XiamenTrajectoryCSVProducer producer = new XiamenTrajectoryCSVProducer(
              path + File.separator + file,
              host,
              port,
              topic,
              "XiamenTrajectoryCSVProducer" + i,
              IntegerSerializer.class.getName(),
              ByteArraySerializer.class.getName(),
              isAsync,
              latch);
      producer.start();
      ++i;
    }
    latch.await();
  }

  private static int getThreadNum(int fileNum) {
    int maxThreads = Runtime.getRuntime().availableProcessors() + 1;
    return Math.min(fileNum, maxThreads);
  }

  private static String[] listFiles(String path) {
    File file = new File(path);
    return file.list();
  }
}
