package com.ebay.sojourner.distributor.function;

import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.connector.kafka.FlinkKafkaProducerFactory;
import com.ebay.sojourner.flink.connector.kafka.KafkaProducerConfig;
import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

@Deprecated
public class SinkDataStreamBuilder<T> {

  private DataStream<T> dataStream;
  private String defaultTopic;
  private DataCenter dc;
  private String slotGroup;
  private String operatorName;
  private String uid;
  private int parallelism = getInteger(Property.SOURCE_PARALLELISM);

  public SinkDataStreamBuilder() {
  }

  public SinkDataStreamBuilder<T> stream(DataStream<T> dataStream) {
    this.dataStream = dataStream;
    return this;
  }

  public SinkDataStreamBuilder<T> dc(DataCenter dc) {
    this.dc = dc;
    return this;
  }

  public SinkDataStreamBuilder<T> topic(String defaultTopic) {
    this.defaultTopic = defaultTopic;
    return this;
  }

  public SinkDataStreamBuilder<T> operatorName(String operatorName) {
    this.operatorName = operatorName;
    return this;
  }

  public SinkDataStreamBuilder<T> uid(String uid) {
    this.uid = uid;
    return this;
  }

  public SinkDataStreamBuilder<T> parallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public SinkDataStreamBuilder<T> slotGroup(String slotGroup) {
    this.slotGroup = slotGroup;
    return this;
  }

  public void build(KafkaSerializationSchema<T> schema) {
    Preconditions.checkNotNull(dc);
    this.build(schema, dc, parallelism, operatorName, uid, slotGroup);
  }

  public void build(KafkaSerializationSchema<T> schema, DataCenter dc, int parallelism,
      String operatorName, String uid, String slotGroup) {
    Preconditions.checkNotNull(dc);
    KafkaProducerConfig config = KafkaProducerConfig.ofDC(dc);
    FlinkKafkaProducerFactory factory = new FlinkKafkaProducerFactory(config);

    dataStream
        .addSink(factory.get(defaultTopic, schema))
        .setParallelism(parallelism)
        .slotSharingGroup(slotGroup)
        .name(operatorName)
        .uid(uid);

  }
}
