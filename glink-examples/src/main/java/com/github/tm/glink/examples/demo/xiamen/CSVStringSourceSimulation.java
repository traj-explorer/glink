package com.github.tm.glink.examples.demo.xiamen;

import com.github.tm.glink.core.enums.TextFileSplitter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

/**
 * @author Wang Haocheng
 * @date 2021/4/22 - 8:09 下午
 */
public class CSVStringSourceSimulation {

    protected String filePath;
    protected BufferedReader bufferedReader;

    // Variables for speed up the simulated stream.
    private Integer speedFactor;
    private long startTime;
    private long startEventTime = -1;
    private long preEventTime;
    private int syncCounter;
    private int timeFieldIndex;
    private TextFileSplitter splitter;
    private boolean withPid; // 如果没有pid，我们需要自行在后面附加。
    private int pid;
    private KafkaProducer kafkaProducer;
    private String topic;

    public CSVStringSourceSimulation(Properties props, String topicid, String filePath, int speedFactor, int timeFieldIndex, TextFileSplitter splitter, boolean withPid) throws FileNotFoundException {
        topic = topicid;
        kafkaProducer = new KafkaProducer(props);
        this.filePath = filePath;
        this.speedFactor = speedFactor;
        this.timeFieldIndex = timeFieldIndex;
        startTime = Instant.now().toEpochMilli();
        this.splitter = splitter;
        this.withPid = withPid;
        FileReader fileReader = new FileReader(filePath);
        bufferedReader = new BufferedReader(fileReader);
    }


    protected void checkTimeAndWait(String line) throws InterruptedException {

        if (timeFieldIndex == -1) {
            return;
        }
        String time = line.split(splitter.getDelimiter())[timeFieldIndex];
        long thisEventTime = Long.parseLong(time);

        if (startEventTime < 0) {
            startEventTime = thisEventTime;
            preEventTime = thisEventTime;
        } else {
            long gapTime = (thisEventTime - preEventTime) / speedFactor;
            if (gapTime > 0 || syncCounter > 1000) {
                long currentTime = System.currentTimeMillis();
                long targetEmitTime = (long) ((thisEventTime - startEventTime) / speedFactor) + startTime;
                long waitTime = targetEmitTime - currentTime;
                if (waitTime > 0) {
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                syncCounter = 0;
            } else {
                syncCounter++;
            }
        }
        preEventTime = thisEventTime;
    }

    public void run() throws Exception {
        String line;
        System.out.println("******** Start sinking data into Kafka. ********");
        while ((line = bufferedReader.readLine()) != null) {
            if (speedFactor > 0)
                checkTimeAndWait(line);
            if (!withPid) {
                kafkaProducer.send(new ProducerRecord(topic, line + "," + pid));
                pid++;
            } else {
                kafkaProducer.send(new ProducerRecord(topic, line));
            }
        }
    }

    public void close() throws IOException {
        bufferedReader.close();
        kafkaProducer.close();
    }
}
