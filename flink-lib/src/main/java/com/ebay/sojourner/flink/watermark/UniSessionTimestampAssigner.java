package com.ebay.sojourner.flink.watermark;

import com.ebay.sojourner.common.model.UniSession;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

@Slf4j
public class UniSessionTimestampAssigner implements SerializableTimestampAssigner<UniSession> {

    @Override
    public long extractTimestamp(UniSession element, long recordTimestamp) {
        try {
            return element.getAbsEndTimestamp();
        } catch (Exception e) {
            log.warn("Parse UniSession absEndTimestamp failed. UniSession record: {}", element, e);
            return System.currentTimeMillis();
        }
    }
}
