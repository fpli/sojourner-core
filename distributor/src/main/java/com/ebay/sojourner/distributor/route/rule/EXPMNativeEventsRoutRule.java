package com.ebay.sojourner.distributor.route.rule;

import com.ebay.sojourner.distributor.route.Route;

@Route(key = "expm-native-events")
public class EXPMNativeEventsRoutRule extends SurfaceEventsRoutRule {
    private final String EXPM_EACTN = "EXPM";

    @Override
    public String getActn() {
        return EXPM_EACTN;
    }
}
