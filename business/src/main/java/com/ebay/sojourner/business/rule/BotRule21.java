package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.util.BotRules;

public class BotRule21 extends AbstractBotRule<AgentIpAttribute> {

  @Override
  public int getBotFlag(AgentIpAttribute agentIpAttribute) {
    if (agentIpAttribute.getIsAgentIp8Bot() > 0) {
      return BotRules.AGENT_IP8_BOT_FLAG;
    } else {
      return BotRules.NON_BOT_FLAG;
    }
  }
}
