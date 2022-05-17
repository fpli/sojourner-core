package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;

@Route(key = "srch-auto-complete")
public class SrchAutoCompleteRouteRule extends AbstractSojEventRouteRule {

    @Override
    public boolean match(SojEvent sojEvent) {
        return sojEvent.getRdt().equals(0)
                && sojEvent.getApplicationPayload().containsKey("trkp")
                && sojEvent.getApplicationPayload().get("trkp").contains("qacc");
    }
}
