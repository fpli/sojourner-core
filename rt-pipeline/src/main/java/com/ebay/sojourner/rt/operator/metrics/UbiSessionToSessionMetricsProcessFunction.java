package com.ebay.sojourner.rt.operator.metrics;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.SojUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class UbiSessionToSessionMetricsProcessFunction extends ProcessFunction<UbiSession, SessionMetrics> {

  @Override
  public void processElement(UbiSession ubiSession, Context context, Collector<SessionMetrics> out) throws Exception {
    SessionMetrics sessionMetrics = SojUtils.extractSessionMetricsFromUbiSession(ubiSession);
    out.collect(sessionMetrics);
  }
}