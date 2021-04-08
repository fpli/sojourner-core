package com.ebay.sojourner.flink.connector.hdfs;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.SojSession;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SojSessionDateTimeBucketAssignerTest {

  SojSessionDateTimeBucketAssigner sojSessionDateTimeBucketAssigner;
  SojSession sojSession;
  Context context;

  @BeforeEach
  void setUp() {
    sojSessionDateTimeBucketAssigner = new SojSessionDateTimeBucketAssigner();
    sojSession = new SojSession();
    sojSession.setSessionStartDt(3804278400000000L);
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
    String bucketId = sojSessionDateTimeBucketAssigner.getBucketId(sojSession, context);
    assertThat(bucketId).isEqualTo("dt=20200721/hr=00");
  }

  @Test
  void getSerializer() {
    sojSessionDateTimeBucketAssigner.getSerializer();
  }

  @Test
  void testToString() {
    sojSessionDateTimeBucketAssigner.toString();
  }
}