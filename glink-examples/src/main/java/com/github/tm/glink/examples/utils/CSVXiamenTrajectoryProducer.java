package com.github.tm.glink.examples.utils;

import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import com.github.tm.glink.kafka.BaseCSVProducer;
import com.github.tm.glink.features.TrajectoryPoint;

import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 * */
public class CSVXiamenTrajectoryProducer extends BaseCSVProducer<String, byte[]> {

  private AvroTrajectoryPoint avroTrajectoryPoint;

  public CSVXiamenTrajectoryProducer(String filePath, String brokerList, String topic, String clientIdConfig, String keySerializer, String valueSerializer, boolean isAsync, CountDownLatch latch) throws FileNotFoundException {
    super(filePath, brokerList, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch, -1);
    String schema = "speed:double;azimuth:int;status:int";
    avroTrajectoryPoint = new AvroTrajectoryPoint(schema);
  }

  @Override
  public KeyValue<String, byte[]> parseLine(String line) {
    String[] items = line.split(",");
    int pid = Integer.parseInt(items[0]);
    String id = items[1];
    double lat = Double.parseDouble(items[2]);
    double lng = Double.parseDouble(items[3]);
    double speed = Double.parseDouble(items[4]);
    int azimuth = Integer.parseInt(items[5]);
    long timestamp = Long.parseLong(items[6]) * 1000;
    int status = Integer.parseInt(items[7]);

    Properties attributes = new Properties();
    attributes.put("speed", speed);
    attributes.put("azimuth", azimuth);
    attributes.put("status", status);
    TrajectoryPoint trajectoryPoint = new TrajectoryPoint(id, pid, lat, lng, timestamp);
    trajectoryPoint.setAttributes(attributes);

    return new KeyValue<>(id, avroTrajectoryPoint.serialize(trajectoryPoint));
  }
}
