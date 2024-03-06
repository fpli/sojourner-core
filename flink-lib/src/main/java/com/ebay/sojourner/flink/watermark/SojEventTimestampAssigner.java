package com.ebay.sojourner.flink.watermark;

import com.ebay.sojourner.common.model.SojEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class SojEventTimestampAssigner implements SerializableTimestampAssigner<SojEvent> {

    @Override
    public long extractTimestamp(SojEvent element, long recordTimestamp) {
        return element.getEventTimestamp();
    }
}
