package com.ebay.sojourner.flink.connector.hdfs;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.SojWatermark;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SojCommonDateTimeBucketAssignerTest {

  SojCommonDateTimeBucketAssigner<SojWatermark> sojCommonDateTimeBucketAssigner;
  SojWatermark sojWatermark;
  Context context;

  @BeforeEach
  void setUp() {
    sojCommonDateTimeBucketAssigner = new SojCommonDateTimeBucketAssigner<>();
    sojWatermark = new SojWatermark(1617859691000L, 0);
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
        return 1617859691000L;
      }
    };
  }

  @Test
  void getBucketId() {
    String bucketId = sojCommonDateTimeBucketAssigner.getBucketId(sojWatermark, context);
    assertThat(bucketId).isEqualTo("dt=20210407/hr=22");
  }

  @Test
  void getSerializer() {
    sojCommonDateTimeBucketAssigner.getSerializer();
  }

  @Test
  void testToString() {
    sojCommonDateTimeBucketAssigner.toString();
  }

}
