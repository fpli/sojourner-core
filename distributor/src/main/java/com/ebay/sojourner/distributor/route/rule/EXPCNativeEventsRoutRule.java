package com.ebay.sojourner.distributor.route.rule;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EACTN;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.google.common.collect.Sets;
import java.util.Set;

@Route(key = "expc-native-events")
public class EXPCNativeEventsRoutRule extends AbstractSojEventRouteRule {

  private final Set<Integer> excludedPageIds = Sets.newHashSet(3818, 2835, 4409);
  private final String EXPC_EACTN = "EXPC";

  @Override
  public boolean match(SojEvent sojEvent) {
    Integer pageId = sojEvent.getPageId();
    String agentInfo = sojEvent.getAgentInfo();

    return pageId != null && !excludedPageIds.contains(sojEvent.getPageId())
        && EXPC_EACTN.equals(sojEvent.getApplicationPayload().get(EACTN))
        && agentInfo != null
        && (agentInfo.startsWith("ebay") || agentInfo.startsWith("eBay"));
  }
}
