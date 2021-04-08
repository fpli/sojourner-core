package com.ebay.sojourner.flink.connector.hdfs;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.SojSession;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HdfsConnectorFactoryTest {

  StreamingFileSink<SojSession> sink;

  @BeforeEach
  void setUp() {
    sink = HdfsConnectorFactory.createWithParquet("test", SojSession.class, new SojSessionDateTimeBucketAssigner());
  }

  @Test
  void createWithParquet() {
    assertThat(sink).isNotNull();
  }
}