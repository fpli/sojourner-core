package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotRules;
import java.util.regex.Pattern;

public class BotRule1 extends AbstractBotRule<UbiSession> {

  private static final Pattern pattern = Pattern.compile(
      ".*bot[^a-z0-9_-].*|.*bot$|.*spider.*|.*crawl.*|.*ktxn.*", Pattern.CASE_INSENSITIVE);
  private static final String CUBOT = "CUBOT";

  private int detectSpiderAgent(UbiSession ubiSession) {
    String agentInfo = ubiSession.getAgentInfo();
    if (ubiSession.getBotFlagList().contains(BotRules.SPIDER_BOT_FLAG)
        || (agentInfo != null
        && pattern.matcher(agentInfo).matches()
        // cubot is phone brand instead of bot
        && !agentInfo.toUpperCase().contains(CUBOT))) {
      return BotRules.SPIDER_BOT_FLAG;
    } else {
      return BotRules.NON_BOT_FLAG;
    }
  }

  @Override
  public int getBotFlag(UbiSession ubiSession) {
    return detectSpiderAgent(ubiSession);
  }
}
