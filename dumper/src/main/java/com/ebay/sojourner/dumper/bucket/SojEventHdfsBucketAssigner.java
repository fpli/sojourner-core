package com.ebay.sojourner.dumper.bucket;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

@Slf4j
public class SojEventHdfsBucketAssigner implements BucketAssigner<SojEvent, String> {

  @Override
  public String getBucketId(SojEvent sojEvent, Context context) {
    StringBuilder sb = new StringBuilder();

    // for event type: bot and nonbot
    if (sojEvent.getBot() != 0) {
      sb.append("/type=bot");
    } else {
      sb.append("/type=nonbot");
    }

    long epochMilli = SojDateTimeUtils.toEpochMilli(sojEvent.getEventTimestamp());

    String dtStr = SojDateTimeUtils.toDateString(epochMilli);
    String hrStr = SojDateTimeUtils.toHrString(epochMilli);

    // for dt and hr
    sb.append("/dt=")
      .append(dtStr);

    sb.append("/hr=")
      .append(hrStr);

    return sb.toString();
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }

}
