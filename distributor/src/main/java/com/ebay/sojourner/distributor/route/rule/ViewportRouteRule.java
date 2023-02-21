package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.VIEWPORT_TAG;

@Route(key = "viewport")
public class ViewportRouteRule extends AbstractSojEventRouteRule {

    @Override
    public boolean match(SojEvent sojEvent) {
        return sojEvent.getApplicationPayload() != null
                && sojEvent.getApplicationPayload().containsKey(VIEWPORT_TAG);
    }
}
