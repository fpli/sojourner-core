package com.ebay.sojourner.business.detector;

import com.ebay.sojourner.business.rule.BotRule206;
import com.ebay.sojourner.business.rule.BotRule208;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotFilter;
import com.ebay.sojourner.common.util.UbiBotFilter;
import com.ebay.sojourner.dsl.domain.rule.Rule;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SessionEndBotDetector implements BotDetector<UbiSession> {

  private static volatile SessionEndBotDetector sessionEndBotDetector;
  private Set<Rule> botRules = new HashSet<>();
  private BotFilter filter = null;

  private SessionEndBotDetector() {
    initBotRules();
    filter = new UbiBotFilter();
    for (Rule rule : botRules) {
      rule.init();
    }
  }

  public static SessionEndBotDetector getInstance() {
    if (sessionEndBotDetector == null) {
      synchronized (SessionEndBotDetector.class) {
        if (sessionEndBotDetector == null) {
          sessionEndBotDetector = new SessionEndBotDetector();
        }
      }
    }
    return sessionEndBotDetector;
  }

  @Override
  public Set<Integer> getBotFlagList(UbiSession ubiSession)
      throws IOException, InterruptedException {
    Set<Integer> botRuleList = new HashSet<>();
    for (Rule rule : botRules) {
      Integer botRule = rule.getBotFlag(ubiSession);
      if (botRule != null && botRule != 0) {
        if (!filter.filter(ubiSession, botRule)) {
          botRuleList.add(botRule);
        }
      }
    }
    return botRuleList;
  }

  @Override
  public void initBotRules() {
    botRules.add(new BotRule206());
    botRules.add(new BotRule208());
    // botRules.add(new BotRule18());
    //botRules.add(new BotRule12End());
  }
}
