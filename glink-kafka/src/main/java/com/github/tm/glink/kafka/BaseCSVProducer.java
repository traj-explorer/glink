package com.github.tm.glink.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yu Liebing
 * */
public abstract class BaseCSVProducer<K, V> extends Thread {

  private KafkaProducer<K, V> producer;
  private String topic;
  private boolean isAsync;
  private CountDownLatch latch;
  private int sleep;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected LinkedList<BufferedReader> bufferedReaders = new LinkedList<>();

  public BaseCSVProducer(final String filePath,
                         final String serverUrl,
                         final int serverPort,
                         final String topic,
                         final String clientIdConfig,
                         final String keySerializer,
                         final String valueSerializer,
                         final boolean isAsync,
                         final CountDownLatch latch,
                         final int sleep) throws FileNotFoundException {
    List<String> filePaths = new ArrayList<>(1);
    filePaths.add(filePath);
    init(filePaths, serverUrl, serverPort, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch, sleep);
  }

  public BaseCSVProducer(final List<String> filePaths,
                         final String serverUrl,
                         final int serverPort,
                         final String topic,
                         final String clientIdConfig,
                         final String keySerializer,
                         final String valueSerializer,
                         final boolean isAsync,
                         final CountDownLatch latch,
                         final int sleep) throws FileNotFoundException {
    init(filePaths, serverUrl, serverPort, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch, sleep);
  }

  public void init(final List<String> filePaths,
                   final String serverUrl,
                   final int serverPort,
                   final String topic,
                   final String clientIdConfig,
                   final String keySerializer,
                   final String valueSerializer,
                   final boolean isAsync,
                   final CountDownLatch latch,
                   final int sleep) throws FileNotFoundException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl + ":" + serverPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdConfig);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

    producer = new KafkaProducer<>(props);
    this.topic = topic;
    this.isAsync = isAsync;
    this.latch = latch;
    this.sleep = sleep;

    for (String file : filePaths) {
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      bufferedReaders.add(bufferedReader);
    }
  }

  public abstract KeyValue<K, V> parseLine(String line);

  @SuppressWarnings("checkstyle:InnerAssignment")
  @Override
  public void run() {
    String line;
    try {
      while (!bufferedReaders.isEmpty()) {
        int size = bufferedReaders.size();
        for (int i = 0; i < size; ++i) {
          BufferedReader bufferedReader = bufferedReaders.pollFirst();
          if (bufferedReader == null) continue;
          if ((line = bufferedReader.readLine()) != null) {
            bufferedReaders.addLast(bufferedReader);
            KeyValue<K, V> keyValue = parseLine(line);
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, keyValue.key, keyValue.value);
            if (isAsync) {
              producer.send(producerRecord, (recordMetadata, e) -> {
                if (recordMetadata != null) {
                  System.out.println("key: " + recordMetadata.topic() + "send at: " + recordMetadata.timestamp());
                } else {
                  System.out.println("failed");
                  e.printStackTrace();
                }
              });
            } else {
              producer.send(producerRecord);
            }
          } else {
            bufferedReader.close();
          }
        }
        if (sleep != -1) {
          Thread.sleep(sleep * 1000);
        }
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    producer.flush();
    latch.countDown();
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static class KeyValue<K, V> {

    public KeyValue(K key, V value) {
      this.key = key;
      this.value = value;
    }

    K key;
    V value;
  }
}
