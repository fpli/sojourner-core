package com.ebay.sojourner.ubd.common.sharedlib.indicators;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class AgentIpIndicatorsSlidingTest {

    @Test
    void getInstance() {
        AgentIpIndicatorsSliding agentIpIndicatorsSliding = AgentIpIndicatorsSliding.getInstance();
        Assertions.assertThat(agentIpIndicatorsSliding.indicators.size()).isEqualTo(2);
    }
}