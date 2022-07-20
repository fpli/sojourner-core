package com.ebay.sojourner.business.detector;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.dsl.domain.rule.Rule;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.powermock.reflect.Whitebox;

class AgentIpSignatureBotDetectorTest {

  AgentIpSignatureBotDetector agentIpSignatureBotDetector;

  @BeforeEach
  void setUp() {
    agentIpSignatureBotDetector = AgentIpSignatureBotDetector.getInstance();
  }


  @Test
  void getBotFlagList() throws Exception {
    AgentIpAttribute agentIpAttribute = new AgentIpAttribute();
    agentIpAttribute.setScsCountForBot5(100);

    Set<Integer> result = agentIpSignatureBotDetector.getBotFlagList(agentIpAttribute);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.contains(5)).isTrue();
  }

  @Test
  void initBotRules() {
    Set<Rule> botRules = Whitebox.getInternalState(agentIpSignatureBotDetector, "botRules", AgentIpSignatureBotDetector.class);
    assertThat(botRules.size()).isEqualTo(4);
  }

}