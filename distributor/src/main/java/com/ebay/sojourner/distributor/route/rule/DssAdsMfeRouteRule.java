package com.ebay.sojourner.distributor.route.rule;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.PLMT;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.google.common.collect.Sets;
import java.util.Set;


@Route(key = "dss-ads-mfe")
public class DssAdsMfeRouteRule extends AbstractSojEventRouteRule {

  private final Set<Integer> MFE_PAGE_IDS = Sets.newHashSet(
      2299321, 2062300, 2053742, 2053444, 2304207,
      2054032, 2317508, 2061037, 2063239, 2296363
  );


  @Override
  public boolean match(SojEvent sojEvent) {
    return MFE_PAGE_IDS.contains(sojEvent.getPageId())
        || sojEvent.getApplicationPayload().get(PLMT) != null;
  }
}
