package com.ebay.sojourner.business.detector;

import com.ebay.sojourner.business.rule.BotRule5;
import com.ebay.sojourner.business.rule.BotRule8;
import com.ebay.sojourner.business.rule.BotRuleForNewBot;
import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.dsl.domain.rule.Rule;
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
    botRules.add(new BotRuleForNewBot());
  }
}
