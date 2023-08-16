package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.IosEventFilter;
import com.ebay.sojourner.common.util.SojEventTimeUtil;
import java.util.Arrays;
import java.util.List;

public class FirstIosHpMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  @Override
  public void init() throws Exception {
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {
    sessionAccumulator.getUbiSession().setFirstIosFgLaunchTimestamp(Long.MAX_VALUE);
    sessionAccumulator.getUbiSession().setFirstIosHpTimestamp(Long.MAX_VALUE);
    sessionAccumulator.getUbiSession().setFirstCollectionExpTimestamp(Long.MAX_VALUE);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {
    List<Integer> iosFgPageIds = Arrays.asList(2051248,2050535);
    int homePageId = 2481888;
    int CollectionExpPageId = 3562572;
    int pageId = event.getPageId();
    UbiSession ubiSession = sessionAccumulator.getUbiSession();

    if (IosEventFilter.isHpOrFgOrLaunchIosEvent(event)) {
      if (iosFgPageIds.contains(pageId) && (SojEventTimeUtil
          .isEarlyEvent(event.getEventTimestamp(), ubiSession.getFirstIosFgLaunchTimestamp()))) {
        ubiSession.setFirstIosFgLaunchTimestamp(event.getEventTimestamp());
      }

      if (pageId == homePageId && (SojEventTimeUtil
          .isEarlyEvent(event.getEventTimestamp(), ubiSession.getFirstIosHpTimestamp()))) {
        ubiSession.setFirstIosHpTimestamp(event.getEventTimestamp());
      }

      if (pageId == CollectionExpPageId && (SojEventTimeUtil
          .isEarlyEvent(event.getEventTimestamp(), ubiSession.getFirstCollectionExpTimestamp()))) {
        ubiSession.setFirstCollectionExpTimestamp(event.getEventTimestamp());
      }
    }

  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }
}
