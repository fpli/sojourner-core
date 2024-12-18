package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.PropertyUtils;
import com.ebay.sojourner.common.util.UBIConfig;
import java.util.Set;

public class LndgPageIdMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  private Set<Integer> invalidPageIds;

  @Override
  public void init() throws Exception {
    invalidPageIds =
        PropertyUtils.getIntegerSet(
            UBIConfig.getString(Property.INVALID_PAGE_IDS), Property.PROPERTY_DELIMITER);
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {
    sessionAccumulator.getUbiSession().setMinSCSeqNum(Integer.MAX_VALUE);
    sessionAccumulator.getUbiSession().setLndgPageId(Integer.MIN_VALUE);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {
    if (!event.isIframe() && !event.isRdt() && !invalidPageIds.contains(event.getPageId())) {
      if (sessionAccumulator.getUbiSession().getMinSCSeqNum() > event.getSeqNum()) {
        sessionAccumulator.getUbiSession().setMinSCSeqNum(event.getSeqNum());
        sessionAccumulator.getUbiSession().setLndgPageId(event.getPageId());
      }
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }
}
