package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.google.common.collect.Sets;

import java.util.Set;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EACTN;

public abstract class SurfaceEventsRoutRule extends AbstractSojEventRouteRule {

    private final Set<Integer> excludedPageIds = Sets.newHashSet(3818, 2835, 4409);

    @Override
    public boolean match(SojEvent sojEvent) {
        Integer pageId = sojEvent.getPageId();
        String agentInfo = sojEvent.getAgentInfo();

        return pageId != null && !excludedPageIds.contains(sojEvent.getPageId())
                && getActn().equals(sojEvent.getApplicationPayload().get(EACTN))
                && agentInfo != null
                && (agentInfo.startsWith("ebay") || agentInfo.startsWith("eBay"));
    }

    public abstract String getActn();

}
