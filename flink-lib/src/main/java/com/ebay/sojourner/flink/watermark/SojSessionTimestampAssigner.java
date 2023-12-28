package com.ebay.sojourner.flink.watermark;

import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class SojSessionTimestampAssigner implements SerializableTimestampAssigner<SojSession> {

  @Override
  public long extractTimestamp(SojSession element, long recordTimestamp) {
    Long absEndTimestamp = element.getAbsEndTimestamp();
    if (absEndTimestamp != null) {
      return SojDateTimeUtils.toEpochMilli(absEndTimestamp);
    } else {
      return System.currentTimeMillis();
    }
  }
}
