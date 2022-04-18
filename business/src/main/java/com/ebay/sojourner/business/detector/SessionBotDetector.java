package com.ebay.sojourner.business.detector;

import com.ebay.sojourner.business.rule.BotRule1;
import com.ebay.sojourner.business.rule.BotRule10;
import com.ebay.sojourner.business.rule.BotRule11;
import com.ebay.sojourner.business.rule.BotRule12;
import com.ebay.sojourner.business.rule.BotRule15;
import com.ebay.sojourner.business.rule.BotRule17;
import com.ebay.sojourner.business.rule.BotRule203;
import com.ebay.sojourner.business.rule.BotRule204;
import com.ebay.sojourner.business.rule.BotRule205;
import com.ebay.sojourner.business.rule.BotRule207;
import com.ebay.sojourner.business.rule.BotRule212;
import com.ebay.sojourner.business.rule.BotRule215;
import com.ebay.sojourner.business.rule.BotRule9;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotFilter;
import com.ebay.sojourner.common.util.UbiBotFilter;
import com.ebay.sojourner.dsl.domain.rule.Rule;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SessionBotDetector implements BotDetector<UbiSession> {

  private static volatile SessionBotDetector sessionBotDetector;
  private Set<Rule> botRules = new HashSet<>();
  private BotFilter filter = null;

  private SessionBotDetector() {
    initBotRules();
    filter = new UbiBotFilter();
    for (Rule rule : botRules) {
      rule.init();
    }
  }

  public static SessionBotDetector getInstance() {
    if (sessionBotDetector == null) {
      synchronized (SessionBotDetector.class) {
        if (sessionBotDetector == null) {
          sessionBotDetector = new SessionBotDetector();
        }
      }
    }
    return sessionBotDetector;
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
    botRules.add(new BotRule1());
    botRules.add(new BotRule15());
    botRules.add(new BotRule9());
    botRules.add(new BotRule10());
    botRules.add(new BotRule12());
    botRules.add(new BotRule203());
    botRules.add(new BotRule204());
    botRules.add(new BotRule205());
    botRules.add(new BotRule207());
    botRules.add(new BotRule212());
    botRules.add(new BotRule215());
    botRules.add(new BotRule11());
    botRules.add(new BotRule17());
  }
}
