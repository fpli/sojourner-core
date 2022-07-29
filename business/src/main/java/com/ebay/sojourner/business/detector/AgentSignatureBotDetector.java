package com.ebay.sojourner.business.detector;

import com.ebay.sojourner.business.rule.BotRule6;
import com.ebay.sojourner.business.rule.BotRuleForSuspectAgent;
import com.ebay.sojourner.business.rule.Rule;
import com.ebay.sojourner.common.model.AgentAttribute;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AgentSignatureBotDetector implements BotDetector<AgentAttribute> {

  private static volatile AgentSignatureBotDetector agentIpSignatureBotDetector;
  private Set<Rule> botRules = new HashSet<>();

  private AgentSignatureBotDetector() {
    initBotRules();
    for (Rule rule : botRules) {
      rule.init();
    }
  }

  public static AgentSignatureBotDetector getInstance() {
    if (agentIpSignatureBotDetector == null) {
      synchronized (AgentSignatureBotDetector.class) {
        if (agentIpSignatureBotDetector == null) {
          agentIpSignatureBotDetector = new AgentSignatureBotDetector();
        }
      }
    }
    return agentIpSignatureBotDetector;
  }

  @Override
  public Set<Integer> getBotFlagList(AgentAttribute agentAttribute)
      throws IOException, InterruptedException {
    Set<Integer> botflagSet = new HashSet<>();
    if (agentAttribute != null) {
      for (Rule rule : botRules) {
        int botFlag = rule.getBotFlag(agentAttribute);
        if (botFlag != 0) {
          botflagSet.add(botFlag);
        }
      }
    }
    return botflagSet;
  }

  @Override
  public void initBotRules() {
    botRules.add(new BotRule6());
    botRules.add(new BotRuleForSuspectAgent());
  }
}
