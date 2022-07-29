package com.ebay.sojourner.business.detector;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.business.rule.Rule;
import com.ebay.sojourner.common.model.AgentAttribute;
import java.util.Set;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.powermock.reflect.Whitebox;

class AgentSignatureBotDetectorTest {

  AgentSignatureBotDetector agentSignatureBotDetector;

  @BeforeEach
  void setUp() {
    agentSignatureBotDetector = AgentSignatureBotDetector.getInstance();
  }

  @Test
  void getBotFlagList() throws Exception {
    AgentAttribute agentAttribute = new AgentAttribute();
    agentAttribute.setScsCount(50);
    agentAttribute.setIpSet(Sets.newHashSet());

    Set<Integer> result = agentSignatureBotDetector.getBotFlagList(agentAttribute);
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.contains(6)).isTrue();
  }

  @Test
  void initBotRules() {
    Set<Rule> botRules = Whitebox.getInternalState(agentSignatureBotDetector, "botRules", AgentSignatureBotDetector.class);
    assertThat(botRules.size()).isEqualTo(2);
  }
}