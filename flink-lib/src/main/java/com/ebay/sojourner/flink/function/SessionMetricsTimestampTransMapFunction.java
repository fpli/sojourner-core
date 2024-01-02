package com.ebay.sojourner.flink.function;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.util.SojTimestamp;
import org.apache.flink.api.common.functions.RichMapFunction;

public class SessionMetricsTimestampTransMapFunction extends RichMapFunction<SessionMetrics, SessionMetrics> {

  @Override
  public SessionMetrics map(SessionMetrics value) throws Exception {

    if (value.getSessionStartDt() != null) {
      value.setSessionStartDt(SojTimestamp.getSojTimestamp(value.getSessionStartDt()));
    }

    if (value.getAbsStartTimestamp() != null) {
      value.setAbsStartTimestamp(SojTimestamp.getSojTimestamp(value.getAbsStartTimestamp()));
    }
    return value;
  }
}
