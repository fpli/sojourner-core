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
            log.warn("Parse SessionMetrics absEndTimestamp failed. SessionMetrics record: {}", element, e);
            return System.currentTimeMillis();
        }
    }
}
