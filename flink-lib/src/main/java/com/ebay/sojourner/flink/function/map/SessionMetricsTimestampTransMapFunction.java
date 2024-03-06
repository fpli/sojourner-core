package com.ebay.sojourner.flink.function.map;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

public class SessionMetricsTimestampTransMapFunction extends RichMapFunction<SessionMetrics, SessionMetrics> {

    @Override
    public SessionMetrics map(SessionMetrics value) throws Exception {

        if (value.getSessionStartDt() != null) {
            value.setSessionStartDt(SojDateTimeUtils.toSojTs(value.getSessionStartDt()));
        }

        if (value.getAbsStartTimestamp() != null) {
            value.setAbsStartTimestamp(SojDateTimeUtils.toSojTs(value.getAbsStartTimestamp()));
        }
        return value;
    }
}
