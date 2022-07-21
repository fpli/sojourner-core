package com.ebay.sojourner.distributor.route.rule;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.SRP_GIST;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;

@Route(key = "srpGist")
public class SrpRouteRule extends AbstractSojEventRouteRule {

    @Override
    public boolean match(SojEvent sojEvent) {
        return sojEvent.getApplicationPayload().containsKey(SRP_GIST);
    }
}
