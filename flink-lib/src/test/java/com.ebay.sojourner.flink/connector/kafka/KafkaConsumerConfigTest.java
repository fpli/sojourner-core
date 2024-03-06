package com.ebay.sojourner.flink.connector.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.flink.common.DataCenter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaConsumerConfigTest {

  KafkaConsumerConfig kafkaConsumerConfig;

  @BeforeEach
  void setUp() {
    kafkaConsumerConfig = null;
  }

  @Test
  void ofDC_lvs() {
    kafkaConsumerConfig = KafkaConsumerConfig.ofDC(DataCenter.LVS);
    assertThat(kafkaConsumerConfig).isNotNull();
    assertThat(kafkaConsumerConfig.getGroupId()).isEqualTo("sojourner-pathfinder-realtime");
    assertThat(kafkaConsumerConfig.getBrokers()).contains(
        "rhs-swsvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-2.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-3.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-4.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-5.rheos-streaming-prod.vip.ebay.com:9092");
  }

  @Test
  void ofDC_rno() {
    kafkaConsumerConfig = KafkaConsumerConfig.ofDC(DataCenter.RNO);
    assertThat(kafkaConsumerConfig).isNotNull();
    assertThat(kafkaConsumerConfig.getGroupId()).isEqualTo("sojourner-pathfinder-realtime");
    assertThat(kafkaConsumerConfig.getBrokers()).contains(
        "rhs-glrvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-2.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-3.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-4.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-5.rheos-streaming-prod.vip.ebay.com:9092");
  }

  @Test
  void ofDC_slc() {
    kafkaConsumerConfig = KafkaConsumerConfig.ofDC(DataCenter.SLC);
    assertThat(kafkaConsumerConfig).isNotNull();
    assertThat(kafkaConsumerConfig.getGroupId()).isEqualTo("sojourner-pathfinder-realtime");
    assertThat(kafkaConsumerConfig.getBrokers()).contains(
        "rhs-mwsvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-mwsvkiaa-kfk-2.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-mwsvkiaa-kfk-3.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-mwsvkiaa-kfk-4.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-mwsvkiaa-kfk-5.rheos-streaming-prod.vip.ebay.com:9092");
  }
}