package com.ebay.sojourner.distributor.route.rule.bullseye;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.ebay.sojourner.distributor.route.rule.AbstractSojEventRouteRule;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "bullseye-guest-checkout")
public class BullseyeGuestCheckoutRouteRule extends AbstractSojEventRouteRule {

  private final Set<Integer> BULLSEYE_GUEST_CHECKOUT_PAGE_IDS = Sets
      .newHashSet(2508507, 2500857, 2503558, 2368482, 2368479, 2368478);

  @Override
  public boolean match(SojEvent sojEvent) {
    return BULLSEYE_GUEST_CHECKOUT_PAGE_IDS.contains(sojEvent.getPageId());
  }
}
