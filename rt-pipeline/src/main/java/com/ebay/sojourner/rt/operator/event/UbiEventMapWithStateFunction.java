package com.ebay.sojourner.rt.operator.event;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Constants;
import java.util.Calendar;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.runtime.operators.windowing.MapWithStateFunction;

@Slf4j
public class UbiEventMapWithStateFunction
    implements MapWithStateFunction<UbiEvent, SessionAccumulator, UbiEvent> {

  @Override
  public UbiEvent map(UbiEvent value, SessionAccumulator sessionAccumulator) throws Exception {

    value.setUserAgent(sessionAccumulator.getUbiSession().getUserAgent());
    value.setIp(sessionAccumulator.getUbiSession().getClientIp());

    if (!value.isNewSession() && sessionAccumulator.getUbiSession().getSessionId() == null) {
      sessionAccumulator.getUbiSession().setSessionId(value.getSessionId());
      sessionAccumulator.getUbiSession().setSessionSkey(value.getSessionSkey());
    } else if (value.isNewSession() && sessionAccumulator.getUbiSession().getSessionId() != null) {
      long sessionSkey = value.getEventTimestamp() / Constants.SESSION_KEY_DIVISION;
      if (sessionSkey < sessionAccumulator.getUbiSession().getSessionSkey()) {
        sessionAccumulator.getUbiSession().setSessionSkey(sessionSkey);
      }
      value.setSessionId(sessionAccumulator.getUbiSession().getSessionId());
      value.setSessionSkey(sessionAccumulator.getUbiSession().getSessionSkey());
    } else if (value.isNewSession() && sessionAccumulator.getUbiSession().getSessionId() == null) {
      value.updateSessionId();
      value.updateSessionSkey();
      sessionAccumulator.getUbiSession().setSessionId(value.getSessionId());
      sessionAccumulator.getUbiSession().setSessionSkey(value.getSessionSkey());
      sessionAccumulator.getUbiSession().setVersion(Constants.SESSION_VERSION);
    }
    if (!value.getSessionId().equals(sessionAccumulator.getUbiSession().getSessionId())) {
      log.debug(Calendar.getInstance().getTime().toLocaleString() +
          System.currentTimeMillis() + "unmatched sessionid between event and session: "
          + "event==" +
          value.getGuid() + " " + value.getSessionSkey() + " " + value.getSessionId()
          + " session===" + sessionAccumulator.getUbiSession().getGuid() + " " +
          sessionAccumulator.getUbiSession().getSessionSkey() + " "
          + sessionAccumulator.getUbiSession().getSessionId());
      value.setSessionId(sessionAccumulator.getUbiSession().getSessionId());
    }
    return value;
  }
}
