package com.ebay.sojourner.distributor.route.rule;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.CP_ID;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "mt-mobile-mc3ur")
public class MTMobilePlatformMc3urRouteRule extends AbstractSojEventRouteRule {

  private final Set<Integer> MC3UR_PAGE_IDS = Sets.newHashSet(
      2058483, 2109664, 2109665, 2056451, 2054060, 2054081
  );

  @Override
  public boolean match(SojEvent sojEvent) {
    Integer pageId = sojEvent.getPageId();
    return pageId != null && (MC3UR_PAGE_IDS.contains(pageId)
        || (pageId.equals(2356359)
              && sojEvent.getApplicationPayload().containsKey(CP_ID)
              && sojEvent.getApplicationPayload().get(CP_ID).equals("2380424"))
        || (pageId.equals(2208336)
              && sojEvent.getApplicationPayload().containsKey(CP_ID)
              && sojEvent.getApplicationPayload().get(CP_ID).equals("2481888")));
  }
}
