package com.ebay.sojourner.flink.connector.hdfs;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.SojEvent;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SojEventDateTimeBucketAssignerTest {

  SojEventDateTimeBucketAssigner sojEventDateTimeBucketAssigner;
  SojEvent sojEvent;
  Context context;

  @BeforeEach
  void setUp() {
    sojEventDateTimeBucketAssigner = new SojEventDateTimeBucketAssigner();
    sojEvent = new SojEvent();
    sojEvent.setEventTimestamp(3804278400000000L);
    context = new Context() {
      @Override
      public long currentProcessingTime() {
        return 0;
      }

      @Override
      public long currentWatermark() {
        return 0;
      }

      @Nullable
      @Override
      public Long timestamp() {
        return null;
      }
    };
  }

  @Test
  void getBucketId() {
    String bucketId = sojEventDateTimeBucketAssigner.getBucketId(sojEvent, context);
    assertThat(bucketId).isEqualTo("dt=20200721/hr=00");
  }

  @Test
  void getSerializer() {
    sojEventDateTimeBucketAssigner.getSerializer();
  }

  @Test
  void testToString() {
    sojEventDateTimeBucketAssigner.toString();
  }

}
