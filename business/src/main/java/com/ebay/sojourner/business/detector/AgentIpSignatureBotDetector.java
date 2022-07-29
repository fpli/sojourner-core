package com.ebay.sojourner.business.detector;

import com.ebay.sojourner.business.rule.*;
import com.ebay.sojourner.common.model.AgentIpAttribute;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AgentIpSignatureBotDetector implements BotDetector<AgentIpAttribute> {

  private static volatile AgentIpSignatureBotDetector agentIpSignatureBotDetector;
  private Set<Rule> botRules = new HashSet<>();

  private AgentIpSignatureBotDetector() {
    initBotRules();
    for (Rule rule : botRules) {
      rule.init();
    }
  }

  public static AgentIpSignatureBotDetector getInstance() {
    if (agentIpSignatureBotDetector == null) {
      synchronized (AgentIpSignatureBotDetector.class) {
        if (agentIpSignatureBotDetector == null) {
          agentIpSignatureBotDetector = new AgentIpSignatureBotDetector();
        }
      }
    }
    return agentIpSignatureBotDetector;
  }

  @Override
  public Set<Integer> getBotFlagList(AgentIpAttribute agentIpAttribute)
      throws IOException, InterruptedException {
    Set<Integer> botflagSet = new HashSet<>();
    if (agentIpAttribute != null) {
      for (Rule rule : botRules) {
        int botFlag = rule.getBotFlag(agentIpAttribute);
        if (botFlag != 0) {
          botflagSet.add(botFlag);
        }
      }
    }
    return botflagSet;
  }

  @Override
  public void initBotRules() {
    botRules.add(new BotRule5());
    botRules.add(new BotRule8());
    //currently we disabled bot rule20 in realtime as there is back pressure issue.
    //botRules.add(new BotRule20());
    botRules.add(new BotRule21());
    botRules.add(new BotRuleForNewBot());
  }
}
