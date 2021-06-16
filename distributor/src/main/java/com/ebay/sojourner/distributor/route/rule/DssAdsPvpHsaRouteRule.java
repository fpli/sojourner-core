package com.ebay.sojourner.distributor.route.rule;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EACTN;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EFAM;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "dss-ads-pvp-hsa")
public class DssAdsPvpHsaRouteRule extends AbstractSojEventRouteRule {

  private final String PVP_HSA_EFAM = "ONEPD";
  private final Set<String> PVP_HSA_EACTNS = Sets.newHashSet("EXPM", "VIEW", "ACTN");

  @Override
  public boolean match(SojEvent sojEvent) {
    return PVP_HSA_EFAM.equals(sojEvent.getApplicationPayload().get(EFAM))
        && PVP_HSA_EACTNS.contains(sojEvent.getApplicationPayload().get(EACTN));
  }
}
