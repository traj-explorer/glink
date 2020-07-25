package com.github.tm.glink.source;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.github.tm.glink.schemas.source.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;

public class DidiGPSPointToKafkaSender {
  private String filepath;
  private int servingSpeed;
  private long dataStartTime;
  private Properties kafkaProps;
  private String topic;

  public DidiGPSPointToKafkaSender(Properties kafkaProps, String topicName, String filePath, int servingSpeed, long dataStartTime) {
    this.filepath = filePath;
    this.topic = topicName;
    this.servingSpeed = servingSpeed;
    this.dataStartTime = dataStartTime;
    this.kafkaProps = kafkaProps;
  }

  public void generateSource() throws IOException, InterruptedException {
    long currentMaxTimeStamp = dataStartTime;
    long waitTime;
    long dataEventTime;
    String line;
    int sum = 0;
    BufferedReader bufferedReader = new BufferedReader(new FileReader(filepath));
    KafkaProducer producer = new KafkaProducer<String, GenericRecord>(kafkaProps);
    // buffer存储使最大时间戳发生变化两条记录的中间的记录。
    // 线程等待只会在当最大时间戳发生变化,且变化量大于10ms时发生。
    LinkedList<ProducerRecord> buffer = new LinkedList<>();
    while (bufferedReader.ready() && (line = bufferedReader.readLine()) != null) {
      ProducerRecord<String, DidiGPSPoint> pr = toProducerRecord(line);
      dataEventTime = Long.parseLong(line.split(",")[2]);
      if (dataEventTime - currentMaxTimeStamp > 10L) {
        // 总等待时间为最大时间戳的变化值。通过除以加速值进行缩短。
        // 存储在buffer中的数据记录将会依次发出。
        waitTime = (dataEventTime - currentMaxTimeStamp) / servingSpeed;
        for (ProducerRecord record : buffer) {
          producer.send(record);
          sum++;
        }
        Thread.sleep(waitTime);
        currentMaxTimeStamp = dataEventTime;
        buffer.clear();
      } else {
        buffer.add(pr);
      }
    }
    // 将buffer中的剩余部分发出。
    for (ProducerRecord record : buffer) {
      producer.send(record);
      sum++;
    }
    bufferedReader.close();
    System.out.println("sum:" + sum);
  }
  private ProducerRecord toProducerRecord(String line) {
    String[] tokens = line.split(",");
    DidiGPSPoint didiGPSPoint = new DidiGPSPoint(tokens[0], tokens[1], Long.parseLong(tokens[2]), Double.parseDouble(tokens[3]), Double.parseDouble(tokens[4]));
    return new ProducerRecord<String, DidiGPSPoint>(topic, didiGPSPoint.getId1().toString(), didiGPSPoint);
  }
}

