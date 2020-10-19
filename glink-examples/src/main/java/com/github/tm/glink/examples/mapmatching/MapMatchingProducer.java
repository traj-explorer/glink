package com.github.tm.glink.examples.mapmatching;

import com.github.tm.glink.examples.utils.CommonUtils;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.io.FileNotFoundException;
import java.util.List;
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

    String[] files = CommonUtils.listFiles(path);
    int threadNum = CommonUtils.getThreadNum(files.length);
    List<List<String>> fileDistributions = CommonUtils.distributionFiles(files, threadNum);
    CountDownLatch latch = new CountDownLatch(threadNum);
    int i = 0;
    for (List<String> threadFiles : fileDistributions) {
      XiamenTrajectoryCSVProducer producer = new XiamenTrajectoryCSVProducer(
              threadFiles,
              host,
              port,
              topic,
              "XiamenTrajectoryCSVProducer" + i,
              IntegerSerializer.class.getName(),
              ByteArraySerializer.class.getName(),
              isAsync,
              latch,
              3);
      producer.start();
      ++i;
    }
    latch.await();
  }
}
