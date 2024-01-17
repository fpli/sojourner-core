package com.ebay.sojourner.flink.watermark;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

@Slf4j
public class RawEventTimestampAssigner implements SerializableTimestampAssigner<RawEvent> {

    @Override
    public long extractTimestamp(RawEvent element, long recordTimestamp) {
        long ts = System.currentTimeMillis();

        try {
            ts = SojDateTimeUtils.toEpochMilli(element.getEventTimestamp());
        } catch (Exception e) {
            log.error("Cannot extract RawEvent timestamp {}", element.getEventTimestamp(), e);
        }

        return ts;
    }
}
