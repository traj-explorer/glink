package com.github.tm.glink.examples.mapmatching;

import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import com.github.tm.glink.kafka.BaseCSVProducer;

import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 */
public class XiamenTrajectoryCSVProducer extends BaseCSVProducer<Integer, byte[]> {

  private AvroTrajectoryPoint avroTrajectoryPoint = new AvroTrajectoryPoint();

  public XiamenTrajectoryCSVProducer(String filePath,
                                     String serverUrl,
                                     int serverPort,
                                     String topic,
                                     String clientIdConfig,
                                     String keySerializer,
                                     String valueSerializer,
                                     boolean isAsync,
                                     CountDownLatch latch) throws FileNotFoundException {
    super(filePath, serverUrl, serverPort, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch);
  }

  @Override
  public KeyValue<Integer, byte[]> parseLine(String line) {
    String[] items = line.split(",");
    int pid = Integer.parseInt(items[0]);
    String carno = items[1];
    double lat = Double.parseDouble(items[2]);
    double lng = Double.parseDouble(items[3]);
    long timestamp = Long.parseLong(items[6]) * 1000;
    TrajectoryPoint point = new TrajectoryPoint(carno, pid, lat, lng, timestamp);
    byte[] data = avroTrajectoryPoint.serialize(point);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return new KeyValue<>(pid, data);
  }
}
