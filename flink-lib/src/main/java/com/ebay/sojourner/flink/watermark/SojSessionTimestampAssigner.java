package com.ebay.sojourner.flink.watermark;

import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

@Slf4j
public class SojSessionTimestampAssigner implements SerializableTimestampAssigner<SojSession> {

    @Override
    public long extractTimestamp(SojSession element, long recordTimestamp) {
        try {
            return SojDateTimeUtils.toEpochMilli(element.getAbsEndTimestamp());
        } catch (Exception e) {
            log.warn("Parse SojSession absEndTimestamp failed. SojSession record: {}", element, e);
            return System.currentTimeMillis();
        }
    }
}
