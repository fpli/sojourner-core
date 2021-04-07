package com.ebay.sojourner.rt.operator.session;

import com.ebay.sojourner.business.detector.SessionBotDetector;
import com.ebay.sojourner.business.metric.SessionMetrics;
import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

@Slf4j
public class UbiSessionAgg
    implements AggregateFunction<UbiEvent, SessionAccumulator, SessionAccumulator> {

  @Override
  public SessionAccumulator createAccumulator() {

    SessionAccumulator sessionAccumulator = new SessionAccumulator();

    try {
      SessionMetrics.getInstance().start(sessionAccumulator);
    } catch (Exception e) {
      log.error("init session metrics failed", e);
    }

    return sessionAccumulator;
  }

  @Override
  public SessionAccumulator add(UbiEvent value, SessionAccumulator accumulator) {
    Set<Integer> eventBotFlagSet = value.getBotFlags();

    try {
      SessionMetrics.getInstance().feed(value, accumulator);
    } catch (Exception e) {
      log.error("start session metrics collection failed", e);
    }

    if (accumulator.getUbiSession().getGuid() == null) {
      accumulator.getUbiSession().setGuid(value.getGuid());
    }

    Set<Integer> sessionBotFlagSet = accumulator.getUbiSession().getBotFlagList();
    try {
      sessionBotFlagSet.addAll(SessionBotDetector.getInstance()
          .getBotFlagList(accumulator.getUbiSession()));
      eventBotFlagSet.addAll(sessionBotFlagSet);
    } catch (Exception e) {
      log.error("start get session botFlagList failed", e);
    }

    accumulator.getUbiSession().setBotFlagList(sessionBotFlagSet);
    value.setBotFlags(eventBotFlagSet);
    return accumulator;
  }

  @Override
  public SessionAccumulator getResult(SessionAccumulator sessionAccumulator) {
    return sessionAccumulator;
  }

  @Override
  public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
    log.info("session accumulator merge");
    a.setUbiSession(a.getUbiSession().merge(b.getUbiSession()));
    return a;
  }
}
