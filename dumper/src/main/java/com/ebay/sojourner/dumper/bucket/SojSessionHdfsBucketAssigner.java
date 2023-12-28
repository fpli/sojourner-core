package com.ebay.sojourner.dumper.bucket;

import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

@Slf4j
public class SojSessionHdfsBucketAssigner implements BucketAssigner<SojSession, String> {

  @Override
  public String getBucketId(SojSession sojSession, Context context) {
    StringBuilder sb = new StringBuilder();

    long sessionEndTimestamp = System.currentTimeMillis();
    long sessionStartDt = System.currentTimeMillis();

    try {
      sessionEndTimestamp = SojDateTimeUtils.toEpochMilli(sojSession.getAbsEndTimestamp());
      sessionStartDt = SojDateTimeUtils.toEpochMilli(sojSession.getSessionStartDt());
    } catch (Exception e) {
      log.warn("session end time is null: " + sessionEndTimestamp);
      log.warn("session start time is null: " + sessionStartDt);
    }

    String sessionEndDtStr = SojDateTimeUtils.toDateString(sessionEndTimestamp);
    String sessionStartDtStr = SojDateTimeUtils.toDateString(sessionStartDt);
    String sessionStartHrStr = SojDateTimeUtils.toHrString(sessionStartDt);

    // for session category
    if (sojSession.getIsOpen()) {
      sb.append("category=open");
    } else if (sessionStartDtStr.equals(sessionEndDtStr)) {
      sb.append("category=sameday");
    } else {
      sb.append("category=crossday");
    }

    // for session type: bot and nonbot
    if (sojSession.getBotFlag() != 0) {
      sb.append("/type=bot");
    } else {
      sb.append("/type=nonbot");
    }

    // for dt and hr
    sb.append("/dt=")
      .append(sessionStartDtStr);

    sb.append("/hr=")
      .append(sessionStartHrStr);

    return sb.toString();
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }

}
