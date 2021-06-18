package com.ebay.sojourner.distributor.route.rule.bullseye;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.ebay.sojourner.distributor.route.rule.AbstractSojEventRouteRule;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "bullseye-ad-type")
public class BullseyeAdtypeRouteRule extends AbstractSojEventRouteRule {

  private final Set<Integer> BULLSEYE_AD_TYPE_PAGE_IDS = Sets
      .newHashSet(2376473, 2493971, 2493972, 2493975, 2493976);

  @Override
  public boolean match(SojEvent sojEvent) {
    return BULLSEYE_AD_TYPE_PAGE_IDS.contains(sojEvent.getPageId());
  }
}
