package com.github.tm.glink.connector.geomesa.options.param;

import com.github.tm.glink.connector.geomesa.options.KafkaConfigOption;
import org.apache.flink.configuration.ReadableConfig;

/**
 * @author Wang Haocheng
 */
public class KafkaDataStoreParam extends GeoMesaDataStoreParam {
  @Override
  public void initFromConfigOptions(ReadableConfig config) {
    super.initFromConfigOptions(config);
    config.getOptional(KafkaConfigOption.KAFKA_BROKERS)
            .ifPresent(v -> params.put(KafkaConfigOption.KAFKA_BROKERS.key(), v));
    config.getOptional(KafkaConfigOption.KAFKA_ZOOKEEPERS)
            .ifPresent(v -> params.put(KafkaConfigOption.KAFKA_ZOOKEEPERS.key(), v));
    config.getOptional(KafkaConfigOption.ZK_PATH)
            .ifPresent(v -> params.put(KafkaConfigOption.ZK_PATH.key(), v));
    config.getOptional(KafkaConfigOption.PRODUCER_CONFIG)
            .ifPresent(v -> params.put(KafkaConfigOption.PRODUCER_CONFIG.key(), v));
    config.getOptional(KafkaConfigOption.CONSUMER_CONFIG)
            .ifPresent(v -> params.put(KafkaConfigOption.CONSUMER_CONFIG.key(), v));
    config.getOptional(KafkaConfigOption.CONSUMER_READ_BACK)
            .ifPresent(v -> params.put(KafkaConfigOption.CONSUMER_READ_BACK.key(), v));
    config.getOptional(KafkaConfigOption.CONSUMER_COUNT)
            .ifPresent(v -> params.put(KafkaConfigOption.CONSUMER_COUNT.key(), v));
    config.getOptional(KafkaConfigOption.CONSUMER_START_ON_DEMAND)
            .ifPresent(v -> params.put(KafkaConfigOption.CONSUMER_START_ON_DEMAND.key(), v));
    config.getOptional(KafkaConfigOption.TOPIC_PARTITIONS)
            .ifPresent(v -> params.put(KafkaConfigOption.TOPIC_PARTITIONS.key(), v));
    config.getOptional(KafkaConfigOption.TOPIC_REPLICATION)
            .ifPresent(v -> params.put(KafkaConfigOption.TOPIC_REPLICATION.key(), v));
    config.getOptional(KafkaConfigOption.SERIALIZATION_TYPE)
            .ifPresent(v -> params.put(KafkaConfigOption.SERIALIZATION_TYPE.key(), v));
    config.getOptional(KafkaConfigOption.CACHE_EXPIRY)
            .ifPresent(v -> params.put(KafkaConfigOption.CACHE_EXPIRY.key(), v));
    config.getOptional(KafkaConfigOption.CACHE_EXPIRY_DYNAMIC)
            .ifPresent(v -> params.put(KafkaConfigOption.CACHE_EXPIRY_DYNAMIC.key(), v));
    config.getOptional(KafkaConfigOption.EVENT_TIME)
            .ifPresent(v -> params.put(KafkaConfigOption.EVENT_TIME.key(), v));
    config.getOptional(KafkaConfigOption.EVENT_TIME_ORDERING)
            .ifPresent(v -> params.put(KafkaConfigOption.EVENT_TIME_ORDERING.key(), v));
    config.getOptional(KafkaConfigOption.INDEX_RESOLUTION_X)
            .ifPresent(v -> params.put(KafkaConfigOption.INDEX_RESOLUTION_X.key(), v));
    config.getOptional(KafkaConfigOption.INDEX_RESOLUTION_Y)
            .ifPresent(v -> params.put(KafkaConfigOption.INDEX_RESOLUTION_Y.key(), v));
    config.getOptional(KafkaConfigOption.INDEX_TIERS)
            .ifPresent(v -> params.put(KafkaConfigOption.INDEX_TIERS.key(), v));
    config.getOptional(KafkaConfigOption.CQENGINE_INDICES)
            .ifPresent(v -> params.put(KafkaConfigOption.CQENGINE_INDICES.key(), v));
    config.getOptional(KafkaConfigOption.LAZY_LOAD)
            .ifPresent(v -> params.put(KafkaConfigOption.LAZY_LOAD.key(), v));
    config.getOptional(KafkaConfigOption.LAZY_FEATURES)
            .ifPresent(v -> params.put(KafkaConfigOption.LAZY_FEATURES.key(), v));
    config.getOptional(KafkaConfigOption.METRICS_REPORTERS)
            .ifPresent(v -> params.put(KafkaConfigOption.METRICS_REPORTERS.key(), v));
  }
}
