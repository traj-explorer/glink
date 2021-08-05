package com.github.tm.glink.examples.demo.xiamen;

import com.github.tm.glink.core.enums.TextFileSplitter;

import java.io.IOException;
import java.util.Properties;

/**
 * Producer，将txt文本发送至kafka topic，可以控制速度。
 * @author Wang Haocheng
 * @date 2021/6/12 - 2:00 下午
 */
public class KafkaDataProducer {
    public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
    public static final String TOPICID = "XiamenData";
    public static final int SPEED_UP = 20;
    public static final int TIMEFIELDINDEX = 3;
    public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", Heatmap.KAFKA_BOOSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        CSVStringSourceSimulation simulation = new CSVStringSourceSimulation(props, TOPICID, FILEPATH, SPEED_UP, TIMEFIELDINDEX, SPLITTER, false);
        simulation.run();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                simulation.close();
            } catch (IOException e) {
                System.err.println("******** Close source Failed! ********");
                e.printStackTrace();
            }
            System.out.println("******** Kafka source closed! ********");
        }));
    }
}
