package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.distributor.route.Route;

@Route(key = "expc-native-events")
public class EXPCNativeEventsRoutRule extends SurfaceEventsRoutRule {
    private final String EXPC_EACTN = "EXPC";

    @Override
    public String getActn() {
        return EXPC_EACTN;
    }
}
