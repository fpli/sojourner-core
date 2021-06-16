package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "dss-gro")
public class DssGroRouteRule extends AbstractSojEventRouteRule {

  private final Set<String> SITE_IDS = Sets.newHashSet("0", "2");

  @Override
  public boolean match(SojEvent sojEvent) {
    return sojEvent.getRdt().equals(0)
        && !sojEvent.getIframe()
        && sojEvent.getBot() == 0
        && SITE_IDS.contains(sojEvent.getSiteId());
  }
}
