package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.AgentAttribute;
import com.ebay.sojourner.common.util.BotRules;

public class BotRule6 extends AbstractBotRule<AgentAttribute> {

  public static final int MAX_COUNT = 50;
  public static final int UPLIMITIPCNT = 20;

  @Override
  public int getBotFlag(AgentAttribute agentAttribute) {
    if (agentAttribute.getScsCount() >= MAX_COUNT
        && agentAttribute.getIpSet().size() <= UPLIMITIPCNT) {
      return BotRules.SCS_ON_AGENT;
    } else {
      return BotRules.NON_BOT_FLAG;
    }
  }
}
