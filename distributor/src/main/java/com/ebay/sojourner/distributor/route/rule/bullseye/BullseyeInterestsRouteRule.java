package com.ebay.sojourner.distributor.route.rule.bullseye;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.ebay.sojourner.distributor.route.rule.AbstractSojEventRouteRule;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "bullseye-interests")
public class BullseyeInterestsRouteRule extends AbstractSojEventRouteRule {

  private final Set<Integer> BULLSEYE_INTERESTS_PAGE_IDS = Sets.newHashSet(2509140);

  @Override
  public boolean match(SojEvent sojEvent) {
    return BULLSEYE_INTERESTS_PAGE_IDS.contains(sojEvent.getPageId());
  }
}
