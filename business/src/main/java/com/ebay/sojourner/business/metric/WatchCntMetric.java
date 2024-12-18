package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.business.parser.PageIndicator;
import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.UBIConfig;
import java.util.Set;

public class WatchCntMetric implements FieldMetrics<UbiEvent, SessionAccumulator> {

  private Set<Integer> searchViewPageSet = null;

  @Override
  public void start(SessionAccumulator sessionAccumulator) {

    sessionAccumulator.getUbiSession().setWatchCoreCnt(0);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
    if (!event.isIframe()
        && searchViewPageSet.contains(event.getPageId())
        && event.getItemId() != null) {
      sessionAccumulator
          .getUbiSession()
          .setWatchCoreCnt(sessionAccumulator.getUbiSession().getWatchCoreCnt() + 1);
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) {
  }

  @Override
  public void init() throws Exception {
    searchViewPageSet = PageIndicator.parse(UBIConfig.getString(Property.SEARCH_VIEW_PAGES));
  }
}
