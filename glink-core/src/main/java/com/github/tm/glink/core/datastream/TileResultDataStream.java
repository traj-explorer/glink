package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.tile.TileResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author Yu Liebing
 */
@Deprecated
public class TileResultDataStream<V> {

  private DataStream<TileResult<V>> tileResultDataStream;

  protected TileResultDataStream(DataStream<TileResult<V>> gridResultDataStream) {
    this.tileResultDataStream = gridResultDataStream;
  }

  public void sinkToKafka(String topic, Properties kafkaProperties) {
    tileResultDataStream.addSink(new FlinkKafkaProducer<TileResult<V>>(
            topic,
            new KafkaSerializationSchema<TileResult<V>>() {
              @Override
              public ProducerRecord<byte[], byte[]> serialize(TileResult<V> tileResult, Long aLong) {
                byte[] value = tileResult.toString().getBytes();
                return new ProducerRecord<>(topic, value);
              }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.NONE));
  }

  public void print() {
    tileResultDataStream.print();
  }
}
