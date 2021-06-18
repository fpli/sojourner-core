package com.ebay.sojourner.distributor.route.rule.bullseye;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.ebay.sojourner.distributor.route.rule.AbstractSojEventRouteRule;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "bullseye-shopping-cart")
public class BullseyeShoppingCartRouteRule extends AbstractSojEventRouteRule {

  private final Set<Integer> BULLSEYE_SHOPPING_CART_PAGE_IDS = Sets.newHashSet(2364840);
  // mapping to jetstream 202,103,305
  private final Set<Integer> BOT_WHITELIST = Sets.newHashSet(7, 11, 12);
  private final Set<String> GUID_WHITELIST
      = Sets.newHashSet("3dced39e1510a62a02c56a55f7254f55");

  @Override
  public boolean match(SojEvent sojEvent) {
    return (sojEvent.getBot() == 0
        || BOT_WHITELIST.contains(sojEvent.getBot())
        || GUID_WHITELIST.contains(sojEvent.getGuid()))
        && BULLSEYE_SHOPPING_CART_PAGE_IDS.contains(sojEvent.getPageId());
  }
}
