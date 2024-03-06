package com.ebay.sojourner.flink.connector.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;

@Deprecated
@Data
@AllArgsConstructor
public class FlinkKafkaSourceConfigWrapper {
  private KafkaConsumerConfig kafkaConsumerConfig;
  private int outOfOrderlessInMin;
  private int idleSourceTimeout;
  private String fromTimestamp;
}
