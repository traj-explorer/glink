package com.github.tm.glink.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

  protected BufferedReader bufferedReader;

  public BaseCSVProducer(final String filePath,
                         final String serverUrl,
                         final int serverPort,
                         final String topic,
                         final String clientIdConfig,
                         final String keySerializer,
                         final String valueSerializer,
                         final boolean isAsync,
                         final CountDownLatch latch) throws FileNotFoundException {
    init(filePath, serverUrl, serverPort, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch);
  }

  public void init(final String filePath,
                   final String serverUrl,
                   final int serverPort,
                   final String topic,
                   final String clientIdConfig,
                   final String keySerializer,
                   final String valueSerializer,
                   final boolean isAsync,
                   final CountDownLatch latch) throws FileNotFoundException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl + ":" + serverPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdConfig);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

    producer = new KafkaProducer<>(props);
    this.topic = topic;
    this.isAsync = isAsync;
    this.latch = latch;

    FileReader fileReader = new FileReader(filePath);
    bufferedReader = new BufferedReader(fileReader);
  }

  public abstract KeyValue<K, V> parseLine(String line);

  @Override
  public void run() {
    String line;
    try {
      while ((line = bufferedReader.readLine()) != null) {
        KeyValue<K, V> keyValue = parseLine(line);
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, keyValue.key, keyValue.value);
        System.out.println(keyValue.key);
        if (isAsync) {
          producer.send(producerRecord, (recordMetadata, e) -> {
            // TODO: do something when failed
          });
        } else {
          producer.send(producerRecord);
        }
      }
      bufferedReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    latch.countDown();
  }

  protected static class KeyValue<K, V> {

    KeyValue(K key, V value) {
      this.key = key;
      this.value = value;
    }

    K key;
    V value;
  }
}
