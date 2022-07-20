package com.ebay.sojourner.business.indicator;

import com.ebay.sojourner.common.model.AgentIpAttributeAccumulator;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.util.BotFilter;
import com.ebay.sojourner.common.util.BotRules;
import com.ebay.sojourner.common.util.UbiBotFilter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentIpIndicators
    extends AttributeIndicators<SessionCore, AgentIpAttributeAccumulator> {

  private static volatile AgentIpIndicators agentIpIndicators;
  private BotFilter botFilter;

  public AgentIpIndicators() {
    botFilter = new UbiBotFilter();
    initIndicators();
    try {
      init();
    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

  public static AgentIpIndicators getInstance() {
    if (agentIpIndicators == null) {
      synchronized (AgentIpIndicators.class) {
        if (agentIpIndicators == null) {
          agentIpIndicators = new AgentIpIndicators();
        }
      }
    }
    return agentIpIndicators;
  }

  @Override
  public void initIndicators() {
    addIndicators(new ScsCntForBot5Indicator<>(botFilter));
    addIndicators(new ScsCntForBot6Indicator(botFilter));
    addIndicators(new ScsCntForBot7Indicator(botFilter));
    addIndicators(new ScsCntForBot8Indicator<>(botFilter));
    //    addIndicators(new NewAgentIPBotIndicator(botFilter, BotRules.AGENT_IP5_BOT_FLAG));
    addIndicators(new NewAgentIPBotIndicator(botFilter, BotRules.AGENT_IP8_BOT_FLAG));
    addIndicators(new SuspectAgentIndicator(botFilter));
    addIndicators(new SuspectIPIndicator(botFilter));
    addIndicators(new NewBotIndicator(botFilter));
  }
}
