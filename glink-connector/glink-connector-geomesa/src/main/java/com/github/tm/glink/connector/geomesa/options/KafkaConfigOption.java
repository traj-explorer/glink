package com.github.tm.glink.connector.geomesa.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author Wang Haocheng
 */
public class KafkaConfigOption extends GeoMesaConfigOption {

  /**
   * Required.
   */
  public static final ConfigOption<String> KAFKA_BROKERS = ConfigOptions
          .key("kafka.brokers")
          .stringType()
          .noDefaultValue()
          .withDescription("Kafka brokers, e.g. “localhost:9092”");

  /**
   * Required.
   */
  public static final ConfigOption<String> KAFKA_ZOOKEEPERS = ConfigOptions
          .key("kafka.zookeepers")
          .stringType()
          .noDefaultValue()
          .withDescription("Kafka zookeepers, e.g “localhost:2181”");

  /**
   * Optional
   */
  public static final ConfigOption<String> ZK_PATH = ConfigOptions
          .key("kafka.zk.path")
          .stringType()
          .noDefaultValue()
          .withDescription("Zookeeper discoverable path, can be used to effectively namespace feature types");

  public static final ConfigOption<String> PRODUCER_CONFIG = ConfigOptions
          .key("kafka.producer.config")
          .stringType()
          .noDefaultValue()
          .withDescription("Configuration options for kafka producer, in Java properties format.");

  public static final ConfigOption<String> CONSUMER_CONFIG = ConfigOptions
          .key("kafka.consumer.config")
          .stringType()
          .noDefaultValue()
          .withDescription("Configuration options for kafka consumer, in Java properties format.");

  public static final ConfigOption<String> CONSUMER_READ_BACK = ConfigOptions
          .key("kafka.consumer.read-back")
          .stringType()
          .noDefaultValue()
          .withDescription("On start up, read messages that were written within this time frame (vs ignore " +
                  "old messages), e.g. ‘1 hour’. Use ‘Inf’ to read all messages. If enabled, features will not " +
                  "be available for query until all existing messages are processed. However, feature listeners " +
                  "will still be invoked as normal.");

  public static final ConfigOption<Integer> CONSUMER_COUNT = ConfigOptions
          .key("kafka.consumer.count")
          .intType()
          .noDefaultValue()
          .withDescription("Number of kafka consumers used per feature type. " +
                  "Set to 0 to disable consuming (i.e. producer only)");

  public static final ConfigOption<Boolean> CONSUMER_START_ON_DEMAND = ConfigOptions
          .key("kafka.consumer.start-on-demand")
          .booleanType()
          .noDefaultValue()
          .withDescription("The default behavior is to start consuming a topic only when that feature type is first requested. " +
                  "This can reduce load if some layers are never queried. Note that care should be taken when setting this to false," +
                  " as the store will immediately start consuming from Kafka for all known feature types, which may require significant memory overhead.");

  public static final ConfigOption<Integer> TOPIC_PARTITIONS = ConfigOptions
          .key("kafka.topic.partitions")
          .intType()
          .noDefaultValue()
          .withDescription("Number of partitions to use in new kafka topics");

  public static final ConfigOption<Integer> TOPIC_REPLICATION = ConfigOptions
          .key("kafka.topic.replication")
          .intType()
          .noDefaultValue()
          .withDescription("Replication factor to use in new kafka topics");

  public static final ConfigOption<String> SERIALIZATION_TYPE = ConfigOptions
          .key("kafka.serialization.type")
          .stringType()
          .noDefaultValue()
          .withDescription("Internal serialization format to use for kafka messages. Must be one of kryo or avro");

  public static final ConfigOption<String> CACHE_EXPIRY = ConfigOptions
          .key("kafka.cache.expiry")
          .stringType()
          .noDefaultValue()
          .withDescription("Expire features from in-memory cache after this delay, e.g. “10 minutes”.");

  public static final ConfigOption<String> CACHE_EXPIRY_DYNAMIC = ConfigOptions
          .key("kafka.cache.expiry.dynamic")
          .stringType()
          .noDefaultValue()
          .withDescription("Expire features dynamically based on CQL predicates. ");

  public static final ConfigOption<String> EVENT_TIME = ConfigOptions
          .key("kafka.cache.event-time")
          .stringType()
          .noDefaultValue()
          .withDescription("Instead of message time, determine expiry based on feature data. " +
                  "This can be an attribute name or a CQL expression, but it must evaluate to a date");

  public static final ConfigOption<Boolean> EVENT_TIME_ORDERING = ConfigOptions
          .key("kafka.cache.event-time.ordering")
          .booleanType()
          .noDefaultValue()
          .withDescription("Instead of message time, determine feature ordering based on event time data.");

  public static final ConfigOption<Integer> INDEX_RESOLUTION_X = ConfigOptions
          .key("kafka.index.resolution.x")
          .intType()
          .noDefaultValue()
          .withDescription("Number of bins in the x-dimension of the spatial index");

  public static final ConfigOption<Integer> INDEX_RESOLUTION_Y = ConfigOptions
          .key("kafka.index.resolution.y")
          .intType()
          .noDefaultValue()
          .withDescription("Number of bins in the x-dimension of the spatial index");

  public static final ConfigOption<String> INDEX_TIERS = ConfigOptions
          .key("kafka.index.tiers")
          .stringType()
          .noDefaultValue()
          .withDescription("Number and size (in degrees) and of tiers to use when indexing geometries with extents");

  public static final ConfigOption<String> CQENGINE_INDICES = ConfigOptions
          .key("kafka.index.cqengine")
          .stringType()
          .noDefaultValue()
          .withDescription("Use CQEngine for indexing individual attributes. Specify as `name:type`, delimited by commas, where name " +
                  "is an attribute and type is one of `default`, `navigable`, `radix`, `unique`, `hash` or `geometry`");

  public static final ConfigOption<Boolean> LAZY_LOAD = ConfigOptions
          .key("kafka.consumer.start-on-demand")
          .booleanType()
          .noDefaultValue()
          .withDescription("The default behavior is to start consuming a topic only when that feature type is first requested. " +
                  "This can reduce load if some layers are never queried. Note that care should be taken when " +
                  "setting this to false, as the store will immediately start consuming from Kafka for all known " +
                  "feature types, which may require significant memory overhead.");

  public static final ConfigOption<Boolean> LAZY_FEATURES = ConfigOptions
          .key("kafka.serialization.lazy")
          .booleanType()
          .noDefaultValue()
          .withDescription("Use lazy deserialization of features. " +
                  "This may improve processing load at the expense of slightly slower query times");

  public static final ConfigOption<String> METRICS_REPORTERS = ConfigOptions
          .key("kafka.metrics.reporters")
          .stringType()
          .noDefaultValue()
          .withDescription("Reporters used to publish Kafka metrics, as TypeSafe config. To use multiple reporters, " +
                  "nest them under the key 'reporters'");
}
