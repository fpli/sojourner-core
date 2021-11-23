package com.ebay.sojourner.flink.connector.kafka;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.SojFlinkKafkaConsumer;

public class FlinkKafkaConsumerFactory {

  private final FlinkKafkaSourceConfigWrapper configWrapper;

  public FlinkKafkaConsumerFactory(FlinkKafkaSourceConfigWrapper configWrapper) {
    this.configWrapper = configWrapper;
  }

  public <T> SojFlinkKafkaConsumer<T> get(KafkaDeserializationSchema<T> deserializer) {

    SojFlinkKafkaConsumer<T> flinkKafkaConsumer = new SojFlinkKafkaConsumer<>(
        configWrapper.getKafkaConsumerConfig().getTopics(),
        deserializer,
        configWrapper.getKafkaConsumerConfig().getProperties());

    if (configWrapper.getOutOfOrderlessInMin() > 0) {

      flinkKafkaConsumer.assignTimestampsAndWatermarks(
          WatermarkStrategy
              .forBoundedOutOfOrderness(Duration.ofMinutes(configWrapper.getOutOfOrderlessInMin()))
              .withTimestampAssigner(new SojSerializableTimestampAssigner())
              .withIdleness(Duration.ofMinutes(configWrapper.getIdleSourceTimeout())));
      /*
      flinkKafkaConsumer.assignTimestampsAndWatermarks(
          new SojBoundedOutOfOrderlessTimestampExtractor<>(
              Time.minutes(config.getOutOfOrderlessInMin())));
              */
    }

    String fromTimestamp = configWrapper.getFromTimestamp();

    if (fromTimestamp.equalsIgnoreCase("earliest")) {
      flinkKafkaConsumer.setStartFromEarliest();
    } else if (Long.parseLong(fromTimestamp) == 0) {
      flinkKafkaConsumer.setStartFromLatest();
    } else if (Long.parseLong(fromTimestamp) > 0) {
      flinkKafkaConsumer.setStartFromTimestamp(Long.parseLong(fromTimestamp));
    } else {
      throw new IllegalArgumentException("Cannot parse fromTimestamp value");
    }

    return flinkKafkaConsumer;
  }
}
