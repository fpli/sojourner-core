package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;

@Route(key = "scandal-trk")
public class ScandalTrkRouteRule extends AbstractSojEventRouteRule {

  private static final Integer PAGE_ID = 3792362;

  @Override
  public boolean match(SojEvent sojEvent) {
    return PAGE_ID.equals(sojEvent.getPageId())
        || (sojEvent.getApplicationPayload() != null
            && (sojEvent.getApplicationPayload().containsKey("scandal_trk")
                || sojEvent.getApplicationPayload().containsKey("scandal_imp")));
  }
}
