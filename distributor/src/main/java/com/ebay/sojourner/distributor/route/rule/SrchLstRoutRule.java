package com.ebay.sojourner.distributor.route.rule;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EACTN;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EFAM;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;

@Route(key = "srch-lst")
public class SrchLstRoutRule extends AbstractSojEventRouteRule {

  private final String SRCH_EFAM = "LST";
  private final String SRCH_EACTN = "SRCH";

  @Override
  public boolean match(SojEvent sojEvent) {
    return SRCH_EFAM.equals(sojEvent.getApplicationPayload().get(EFAM))
        && SRCH_EACTN.equals(sojEvent.getApplicationPayload().get(EACTN));
  }
}
