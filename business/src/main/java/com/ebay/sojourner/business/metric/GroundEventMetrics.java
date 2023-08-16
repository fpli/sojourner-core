package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.IosEventFilter;

import java.util.Arrays;
import java.util.List;

/**
 * foregroundevent page id: 2050494,1673581,1698105,2034596,2054180,2051248,2050605,2050535
 * backgroundevent page id: 2050495,2051249,2050606
 *
 * @author donlu
 */
public class GroundEventMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  @Override
  public void init() throws Exception {
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {
    sessionAccumulator.getUbiSession().setIsExistForegroundEvent(0);
    sessionAccumulator.getUbiSession().setIsExistBackgroundEvent(0);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {

    UbiSession ubiSession = sessionAccumulator.getUbiSession();
    // To check whether the event is foreground or background event
    List<Integer> FOREGROUND_PAGE_IDS = Arrays.asList(2050494,1673581,1698105,2034596,2054180,2051248,2050605,2050535);
    List<Integer> BACKGROUND_PAGE_IDS = Arrays.asList(2050495,2051249,2050606);
    if (IosEventFilter.isIosEvent(event.getAgentInfo())) {
      if (FOREGROUND_PAGE_IDS.contains(event.getPageId())) {
        ubiSession.setIsExistForegroundEvent(1);
      }
      if (BACKGROUND_PAGE_IDS.contains(event.getPageId())) {
        ubiSession.setIsExistBackgroundEvent(1);
      }
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }
}
