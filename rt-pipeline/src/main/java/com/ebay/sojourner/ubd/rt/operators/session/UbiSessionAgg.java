package com.ebay.sojourner.ubd.rt.operators.session;

import com.ebay.sojourner.ubd.common.model.SessionAccumulator;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.sharedlib.detectors.SessionBotDetector;
import com.ebay.sojourner.ubd.common.sharedlib.metrics.SessionMetrics;
import java.io.IOException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.log4j.Logger;

@Slf4j
public class UbiSessionAgg
    implements AggregateFunction<UbiEvent, SessionAccumulator, SessionAccumulator> {

  //    private CouchBaseManager couchBaseManager;
  //    private static final String BUCKET_NAME="botsignature";
  private static final Logger logger = Logger.getLogger(UbiSessionAgg.class);
  private transient SessionMetrics sessionMetrics;
  private transient SessionBotDetector sessionBotDetector;

  @Override
  public SessionAccumulator createAccumulator() {
    SessionAccumulator sessionAccumulator = new SessionAccumulator();
    sessionMetrics = SessionMetrics.getInstance();
    sessionBotDetector = SessionBotDetector.getInstance();
    //        couchBaseManager = CouchBaseManager.getInstance();
    try {
      sessionMetrics.start(sessionAccumulator);
    } catch (Exception e) {
      log.error("session metrics start fail", e);
    }
    return sessionAccumulator;
  }

  @Override
  public SessionAccumulator add(UbiEvent value, SessionAccumulator accumulator) {
    Set<Integer> eventBotFlagSet = value.getBotFlags();

    try {
      // move this logic to MapWithStateFunction
      //            if (value.isNewSession() && accumulator.getUbiSession().getSessionId() == null)
      // {
      //                value.updateSessionId();
      //            }
      sessionMetrics.feed(value, accumulator);
    } catch (Exception e) {
      log.error(
          "start-session metrics collection issue:"
              + value.getGuid()
              + "||"
              + (value.getSessionId() == null ? "" : value.getSessionId())
              + "||"
              + value.getSeqNum(),
          e);
    }
    if (accumulator.getUbiSession().getGuid() == null) {
      accumulator.getUbiSession().setGuid(value.getGuid());
    }
    Set<Integer> sessionBotFlagSetDetect = null;
    try {
      sessionBotFlagSetDetect = sessionBotDetector.getBotFlagList(accumulator.getUbiSession());
    } catch (IOException | InterruptedException e) {
      log.error("sessionBotDetector getBotFlagList error", e);
    }

    Set<Integer> sessionBotFlagSet = accumulator.getUbiSession().getBotFlagList();

    //        Set<Integer> attrBotFlagWithIp =
    // couchBaseManager.getSignatureWithDocId(accumulator.getUbiSession().getClientIp());
    //        Set<Integer> attrBotFlagWithAgentIp =
    // couchBaseManager.getSignatureWithDocId(accumulator.getUbiSession().getUserAgent()
    // +accumulator.getUbiSession().getClientIp());
    //        Set<Integer> attrBotFlagWithAgent =
    // couchBaseManager.getSignatureWithDocId(accumulator.getUbiSession().getUserAgent());

    if (eventBotFlagSet != null
        && eventBotFlagSet.size() > 0
        && !sessionBotFlagSet.containsAll(eventBotFlagSet)) {
      sessionBotFlagSet.addAll(eventBotFlagSet);
    }
    if (sessionBotFlagSetDetect != null && sessionBotFlagSetDetect.size() > 0) {

      sessionBotFlagSet.addAll(sessionBotFlagSetDetect);
      eventBotFlagSet.addAll(sessionBotFlagSetDetect);
    }

    accumulator.getUbiSession().setBotFlagList(sessionBotFlagSet);
    value.setBotFlags(eventBotFlagSet);
    //        accumulator.setUbiEvent(value);

    return accumulator;
  }

  @Override
  public SessionAccumulator getResult(SessionAccumulator sessionAccumulator) {
    return sessionAccumulator;
  }

  @Override
  public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
    log.info("SessionAccumulator merge:");
    a.setUbiSession(a.getUbiSession().merge(b.getUbiSession()));
    return a;
  }
}
