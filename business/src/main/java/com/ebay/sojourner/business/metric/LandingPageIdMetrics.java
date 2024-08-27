package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.business.parser.PageIndicator;
import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.SojEventTimeUtil;
import com.ebay.sojourner.common.util.UBIConfig;

public class LandingPageIdMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  private PageIndicator indicator = null;

  @Override
  public void start(SessionAccumulator sessionAccumulator) {
    sessionAccumulator.getUbiSession().setLandingPageId(Integer.MIN_VALUE);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
    boolean isEarlyValidEvent = event.isPartialValidPageFlag() && SojEventTimeUtil
        .isEarlyEvent(event.getEventTimestamp(),
            sessionAccumulator.getUbiSession().getStartTimestampForPartialValidPage());
    if (!event.isIframe()
            && !event.isRdt()
            && event.getPageId() != -1
            && (sessionAccumulator.getUbiSession().getLandingPageId() == Integer.MIN_VALUE || isEarlyValidEvent)) {
            sessionAccumulator.getUbiSession().setLandingPageId(event.getPageId());
        }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) {
    if (sessionAccumulator.getUbiSession().getStartPageId() == Integer.MIN_VALUE) {
      sessionAccumulator.getUbiSession().setLandingPageId(1);
    }
  }

  @Override
  public void init() throws Exception {
    setPageIndicator(new PageIndicator(UBIConfig.getString(Property.BOT_BLOCKER_PAGES)));
  }

  void setPageIndicator(PageIndicator indicator) {
    this.indicator = indicator;
  }
}
