package com.github.tm.glink.kafka;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.avro.AvroPoint;

import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;

/**
 * This class is used to produce point objects from csv files
 * to Kafka. In each csv file, one line represents a point,
 * and each line contains the following four elements:
 * 1. id, will be parsed as <code>String<code/>
 * 2. lat, will be parsed as <code>double<code/>
 * 3. lng, will be parsed as <code>double</code>
 * 4. time, the format is yyyy-MM-dd HH:mm:ss
 * Each element is separated by a comma.
 *
 * @author Yu Liebing
 */
public class CSVPointProducer extends BaseCSVProducer<String, byte[]> {

  private AvroPoint avroPoint = new AvroPoint();
  private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public CSVPointProducer(String filePath,
                          String serverUrl,
                          int serverPort,
                          String topic,
                          String clientIdConfig,
                          String keySerializer,
                          String valueSerializer,
                          boolean isAsync,
                          CountDownLatch latch,
                          final int sleep) throws FileNotFoundException {
    super(filePath, serverUrl, serverPort, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch, sleep);
  }

  public CSVPointProducer(String filePath,
                          String serverUrl,
                          int serverPort,
                          String topic,
                          String clientIdConfig,
                          String keySerializer,
                          String valueSerializer,
                          boolean isAsync,
                          CountDownLatch latch) throws FileNotFoundException {
    super(filePath, serverUrl, serverPort, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch, -1);
  }

  @Override
  public KeyValue<String, byte[]> parseLine(String line) {
    String[] items = line.split(",");
    long timestamp = LocalDateTime.parse(items[3], formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    Point point = new Point(items[0], Double.parseDouble(items[1]), Double.parseDouble(items[2]), timestamp);
    byte[] data = avroPoint.serialize(point);
    return new KeyValue<>(items[0], data);
  }
}
