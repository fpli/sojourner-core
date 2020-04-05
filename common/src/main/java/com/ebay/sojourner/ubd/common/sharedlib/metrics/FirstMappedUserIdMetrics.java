package com.ebay.sojourner.ubd.common.sharedlib.metrics;

import com.ebay.sojourner.ubd.common.model.SessionAccumulator;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.model.UbiSession;
import com.ebay.sojourner.ubd.common.sharedlib.util.SOJNVL;

public class FirstMappedUserIdMetrics implements FieldMetrics<UbiEvent, SessionAccumulator>,
    EventListener {

  @Override
  public void start(SessionAccumulator sessionAccumulator) {
    sessionAccumulator.getUbiSession().setFirstMappedUserId(null);
  }

  /**
   * get first valid bu tag in application_payload of valid events
   */
  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
    String bestGuessUserId = null;
    if (sessionAccumulator.getUbiSession().getFirstMappedUserId() == null
        && !event.isRdt()
        && !event.isIframe()) {
      bestGuessUserId = SOJNVL.getTagValue(event.getApplicationPayload(), "bu");
      try {
        bestGuessUserId = bestGuessUserId.trim();
        if (bestGuessUserId.length() <= 18) {
          Long validUserId = Long.parseLong(bestGuessUserId);
          if (validUserId > 0 && validUserId != 3564) {
            sessionAccumulator.getUbiSession().setFirstMappedUserId(validUserId);
          }
        }
      } catch (NumberFormatException e1) {
        // ignore
      } catch (NullPointerException e2) {
        // ignore
      }
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) {
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }

  @Override
  public void onEarlyEventChange(UbiEvent ubiEvent, UbiSession ubiSession) {
    String bestGuessUserId = null;
    if (!ubiEvent.isRdt()
        && !ubiEvent.isIframe()) {
      bestGuessUserId = SOJNVL.getTagValue(ubiEvent.getApplicationPayload(), "bu");
      try {
        bestGuessUserId = bestGuessUserId.trim();
        if (bestGuessUserId.length() <= 18) {
          Long validUserId = Long.parseLong(bestGuessUserId);
          if (validUserId > 0 && validUserId != 3564) {
            ubiSession.setFirstMappedUserId(validUserId);
          }
        }
      } catch (NumberFormatException e1) {
        // ignore
      } catch (NullPointerException e2) {
        // ignore
      }
    }
  }

  @Override
  public void onLateEventChange(UbiEvent ubiEvent, UbiSession ubiSession) {

  }
}
