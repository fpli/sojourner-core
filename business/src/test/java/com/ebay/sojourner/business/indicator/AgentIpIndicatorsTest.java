package com.ebay.sojourner.business.indicator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class AgentIpIndicatorsTest {

  @Test
  void test_getInstance() {
    AgentIpIndicators agentIpIndicators = AgentIpIndicators.getInstance();
    Assertions.assertThat(agentIpIndicators.indicators.size()).isEqualTo(8);
  }
}
