package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.IosEventFilter;
import com.ebay.sojourner.common.util.SojEventTimeUtil;

public class ValidPageIosMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  @Override
  public void init() throws Exception {
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {
    sessionAccumulator.getUbiSession().setValidPageCnt(0);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {
    UbiSession ubiSession = sessionAccumulator.getUbiSession();
    long timestamp = event.getEventTimestamp();

    boolean iosInvalid = ubiSession.getFirstIosFgLaunchTimestamp() != Long.MAX_VALUE
          && SojEventTimeUtil.isEarlyEvent(timestamp, ubiSession.getFirstIosFgLaunchTimestamp())
        || ubiSession.getFirstCollectionExpTimestamp() != Long.MAX_VALUE
          && timestamp == ubiSession.getFirstIosHpTimestamp()
          && ubiSession.getFirstIosFgLaunchTimestamp() == Long.MAX_VALUE;
    if (!iosInvalid && IosEventFilter.isValidIosEvent(event)) {
      ubiSession.setValidPageCntForIos(ubiSession.getValidPageCntForIos() + 1);

      if (SojEventTimeUtil
          .isEarlyEvent(timestamp, ubiSession.getStartTimestampForValidPageIos())) {
        ubiSession.setLndgPageIdForIos(event.getPageId());
        ubiSession.setStartTimestampForValidPageIos(timestamp);
      }

      if (SojEventTimeUtil
          .isLateEvent(timestamp, ubiSession.getEndTimestampForValidPageIos())) {
        ubiSession.setExitPageIdForIos(event.getPageId());
        ubiSession.setEndTimestampForValidPageIos(timestamp);
      }

    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }
}
