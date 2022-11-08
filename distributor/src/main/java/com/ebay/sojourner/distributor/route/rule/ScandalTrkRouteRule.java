package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;

@Route(key = "scandal-trk")
public class ScandalTrkRouteRule extends AbstractSojEventRouteRule {

  @Override
  public boolean match(SojEvent sojEvent) {
    return sojEvent.getApplicationPayload() != null
        && sojEvent.getApplicationPayload().containsKey("scandal_trk");
  }
}
