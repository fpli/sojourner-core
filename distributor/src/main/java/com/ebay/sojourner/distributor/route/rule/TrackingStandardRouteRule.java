package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.google.common.collect.Sets;

import java.util.Set;

@Route(key = "new-tracking-standard")
public class TrackingStandardRouteRule extends AbstractSojEventRouteRule {

    private static final Set<String> complexTags = Sets.newHashSet(
            "srpGist",
            "cassini",
            "experience",
            "viewport",
            "interaction",
            "cartopstate",
            "cjsBeta",
            "cjs"
    );


    @Override
    public boolean match(SojEvent sojEvent) {
        for (String complexTag : complexTags) {
            if (sojEvent.getApplicationPayload() != null && sojEvent.getApplicationPayload().containsKey(complexTag)) {
                return true;
            }
        }
        return false;
    }
}
