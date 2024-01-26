package com.ebay.sojourner.flink.watermark;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

@Slf4j
public class SessionMetricsTimestampAssigner implements SerializableTimestampAssigner<SessionMetrics> {

    @Override
    public long extractTimestamp(SessionMetrics element, long recordTimestamp) {
        try {
            return SojDateTimeUtils.toEpochMilli(element.getAbsEndTimestamp());
        } catch (Exception e) {
            log.warn("failed session record: {}; guid: {}; sessionskey: {}",
                     element.toString(), element.getGuid(), element.getSessionSkey());
            log.warn("parse session-metrics startdt failed: ", e);
            return System.currentTimeMillis();
        }
    }
}
