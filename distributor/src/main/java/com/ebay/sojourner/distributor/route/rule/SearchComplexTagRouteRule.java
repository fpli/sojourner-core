package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.route.Route;
import com.google.common.collect.Sets;

import java.util.Set;

@Route(key = "search-complex-tag")
public class SearchComplexTagRouteRule extends AbstractSojEventRouteRule {

    private static final Set<String> complexTags = Sets.newHashSet(
            "srpGist",
            "cassini",
            "experience",
            "interaction",
            "cartopstate"
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
