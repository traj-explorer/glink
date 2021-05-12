package com.github.tm.glink.examples.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaUtils {

  public static Set<String> listTopics(Properties properties) throws Exception {
    AdminClient adminClient = AdminClient.create(properties);
    return adminClient.listTopics().names().get();
  }

  public static boolean isTopicExists(String topicName, Properties properties) throws Exception {
    Set<String> topics = listTopics(properties);
    return topics.contains(topicName);
  }

  public static void deleteTopic(String topicName, Properties properties) throws Exception {
    if (!isTopicExists(topicName, properties)) return;
    AdminClient adminClient = AdminClient.create(properties);
    adminClient.deleteTopics(Collections.singletonList(topicName));
    Thread.sleep(1000);
  }

  public static void createTopic(String topicName,
                                 int numPartitions,
                                 int replicationFactor,
                                 Properties properties,
                                 Map<String, String> configs) throws Exception {
    deleteTopic(topicName, properties);
    AdminClient adminClient = AdminClient.create(properties);
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
    newTopic.configs(configs);
    adminClient.createTopics(Collections.singletonList(newTopic));
    Thread.sleep(1000);
  }
}
