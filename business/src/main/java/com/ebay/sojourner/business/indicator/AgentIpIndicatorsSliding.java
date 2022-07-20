package com.ebay.sojourner.business.indicator;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.AgentIpAttributeAccumulator;
import com.ebay.sojourner.common.util.BotFilter;
import com.ebay.sojourner.common.util.BotRules;
import com.ebay.sojourner.common.util.UbiBotFilter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentIpIndicatorsSliding
    extends AttributeIndicators<AgentIpAttribute, AgentIpAttributeAccumulator> {

  private static volatile AgentIpIndicatorsSliding agentIpIndicators;
  private BotFilter botFilter;

  public AgentIpIndicatorsSliding() {
    botFilter = new UbiBotFilter();
    initIndicators();
    try {
      init();
    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

  public static AgentIpIndicatorsSliding getInstance() {
    if (agentIpIndicators == null) {
      synchronized (AgentIpIndicatorsSliding.class) {
        if (agentIpIndicators == null) {
          agentIpIndicators = new AgentIpIndicatorsSliding();
        }
      }
    }
    return agentIpIndicators;
  }

  @Override
  public void initIndicators() {
    addIndicators(new ScsCntForBot5Indicator<>(botFilter));
    addIndicators(new ScsCntForBot8Indicator<>(botFilter));
    //  addIndicators(new NewAgentIPBotIndicator<>(botFilter, BotRules.AGENT_IP5_BOT_FLAG));
    addIndicators(new NewAgentIPBotIndicator<>(botFilter, BotRules.AGENT_IP8_BOT_FLAG));
  }
}
