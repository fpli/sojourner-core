package com.ebay.sojourner.flink.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlinkEnvUtilsTest {

  @BeforeEach
  void setUp() {
    FlinkEnvUtils.prepare(new String[]{"--profile", "test"});
  }

  @Test
  void prepare() {
    assertThat(FlinkEnvUtils.getString("profile")).isEqualTo("test");
  }

  @Test
  void getString() {
    assertThat(FlinkEnvUtils.getString("flink.app.name")).isEqualTo("Sojourner Test");
  }

  @Test
  void getInteger() {
    assertThat(FlinkEnvUtils.getInteger("flink.app.checkpoint.interval-ms")).isEqualTo(300000);
  }

  @Test
  void getBoolean() {
    assertThat(FlinkEnvUtils.getBoolean("flink.app.hot-deploy")).isFalse();
  }

  @Test
  void getListString() {
    assertThat(FlinkEnvUtils.getListString("kafka.consumer.bootstrap-servers.rno")).isEqualTo(
        String.join(",", Lists.newArrayList(
            "rhs-glrvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
            "rhs-glrvkiaa-kfk-2.rheos-streaming-prod.vip.ebay.com:9092",
            "rhs-glrvkiaa-kfk-3.rheos-streaming-prod.vip.ebay.com:9092",
            "rhs-glrvkiaa-kfk-4.rheos-streaming-prod.vip.ebay.com:9092",
            "rhs-glrvkiaa-kfk-5.rheos-streaming-prod.vip.ebay.com:9092"
        )));
  }

  @Test
  void getSet() {
    assertThat(FlinkEnvUtils.getSet("kafka.consumer.bootstrap-servers.lvs").size()).isEqualTo(5);
    assertThat(FlinkEnvUtils.getSet("kafka.consumer.bootstrap-servers.lvs")).contains(
        "rhs-swsvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-swsvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092");
  }

  @Test
  void getList() {
    assertThat(FlinkEnvUtils.getList("kafka.consumer.bootstrap-servers.rno").size()).isEqualTo(5);
    assertThat(FlinkEnvUtils.getList("kafka.consumer.bootstrap-servers.rno")).contains(
        "rhs-glrvkiaa-kfk-1.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-2.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-3.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-4.rheos-streaming-prod.vip.ebay.com:9092",
        "rhs-glrvkiaa-kfk-5.rheos-streaming-prod.vip.ebay.com:9092");
  }
}