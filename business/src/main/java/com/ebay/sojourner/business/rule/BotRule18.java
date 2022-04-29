package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotRules;

public class BotRule18 extends AbstractBotRule<UbiSession> {

  @Override
  public int getBotFlag(UbiSession ubiSession) {
    String firstAgent = ubiSession.getFirstAgent();
    String lastAgent = ubiSession.getLastAgent();

    if (firstAgent != null && !isNativeAgent(firstAgent) && !isOtherSpecialAgent(firstAgent)
        && lastAgent != null && !isOtherSpecialAgent(lastAgent) && !firstAgent.equals(lastAgent)) {
      return BotRules.AGENT_HOPPING_BOT_FLAG;
    } else {
      return BotRules.NON_BOT_FLAG;
    }
  }


  public boolean isNativeAgent(String agent) {
    if (agent.startsWith("eBay") || agent.startsWith("ebay")) {
      return true;
    }
    return false;
  }

  public boolean isOtherSpecialAgent(String agent) {
    if (agent.contains("OPR/") || agent.startsWith("GingerClient")) {
      return true;
    }
    return false;
  }
}
