package com.ebay.sojourner.business.ubd.metrics;

import com.ebay.sojourner.common.util.SojEventTimeUtil;
import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Constants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionStartDtMetrics implements FieldMetrics<UbiEvent, SessionAccumulator>,
    EventListener {

  @Override
  public void init() throws Exception {
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) {
    sessionAccumulator.getUbiSession().setSessionStartDt(null);
    sessionAccumulator.getUbiSession().setSeqNum(0);
    sessionAccumulator.getUbiSession().setFirstSessionStartDt(null);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
    boolean isEarlyEvent = SojEventTimeUtil
        .isEarlyEvent(event.getEventTimestamp(),
            sessionAccumulator.getUbiSession().getAbsStartTimestamp());
    boolean isEarlyValidEvent = SojEventTimeUtil
        .isEarlyEvent(event.getEventTimestamp(),
            sessionAccumulator.getUbiSession().getStartTimestampNOIFRAMERDT());

    sessionAccumulator
        .getUbiSession()
        .setSeqNum(sessionAccumulator.getUbiSession().getSeqNum() + 1);
    if (isEarlyEvent ? isEarlyEvent
        : sessionAccumulator.getUbiSession().getFirstSessionStartDt() == null) {
      sessionAccumulator.getUbiSession().setFirstSessionStartDt(event.getSojDataDt());
    }
    if (!event.isIframe()
        && !event.isRdt()
        && (isEarlyValidEvent ? isEarlyValidEvent
        : sessionAccumulator.getUbiSession().getSessionStartDt() == null)) {
      sessionAccumulator.getUbiSession().setSessionStartDt(event.getSojDataDt());
    }
    if (isEarlyEvent) {
      long sessionSkey = event.getEventTimestamp() / Constants.SESSION_KEY_DIVISION;
      sessionAccumulator.getUbiSession().setSessionSkey(sessionSkey);
    }

    if (!event.isNewSession() && sessionAccumulator.getUbiSession().getSessionId() == null) {
      sessionAccumulator.getUbiSession().setSessionId(event.getSessionId());
      sessionAccumulator.getUbiSession().setSessionSkey(event.getSessionSkey());
    } else if (event.isNewSession() && sessionAccumulator.getUbiSession().getSessionId() != null) {
      event.setSessionId(sessionAccumulator.getUbiSession().getSessionId());
      event.setSessionSkey(sessionAccumulator.getUbiSession().getSessionSkey());
    } else if (event.isNewSession() && sessionAccumulator.getUbiSession().getSessionId() == null) {
      event.updateSessionId();
      event.updateSessionSkey();
      sessionAccumulator.getUbiSession().setSessionId(event.getSessionId());
      sessionAccumulator.getUbiSession().setSessionSkey(event.getSessionSkey());
      sessionAccumulator.getUbiSession().setVersion(Constants.SESSION_VERSION);
    }

    event.setSessionStartDt(
        sessionAccumulator.getUbiSession().getSessionStartDt() == null ? sessionAccumulator
            .getUbiSession().getFirstSessionStartDt()
            : sessionAccumulator.getUbiSession().getSessionStartDt());
    event.setSeqNum(sessionAccumulator.getUbiSession().getSeqNum());
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) {
    if (sessionAccumulator.getUbiSession().getSessionStartDt() == null
        && sessionAccumulator.getUbiSession().getFirstSessionStartDt() != null) {
      sessionAccumulator.getUbiSession()
          .setSessionStartDt(sessionAccumulator.getUbiSession().getFirstSessionStartDt());
    }
  }

  @Override
  public void onEarlyEventChange(UbiEvent ubiEvent, UbiSession ubiSession) {
    ubiSession.setFirstSessionStartDt(ubiEvent.getSojDataDt());
    long sessionSkey = ubiEvent.getEventTimestamp() / Constants.SESSION_KEY_DIVISION;
    //  System.out.println("===========onEarlyEventChange ==========");
    //  System.out.println("event guid:"+ubiEvent.getGuid());
    //  System.out.println("event sessionId:"+ubiEvent.getSessionId());
    // System.out.println("before update session sessionskey:"+ubiSession.getSessionSkey());
    ubiSession.setSessionSkey(sessionSkey);
    ubiEvent.setSessionSkey(sessionSkey);
    //  System.out.println("after update session sessionskey:"+ubiSession.getSessionSkey());

  }

  @Override
  public void onLateEventChange(UbiEvent ubiEvent, UbiSession ubiSession) {

  }
}