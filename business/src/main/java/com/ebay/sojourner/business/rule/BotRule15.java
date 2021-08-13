package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotRules;

public class BotRule15 extends AbstractBotRule<UbiSession> {

  public static final int MANY_EVENTS_BOUND = 10000;

  @Override
  public int getBotFlag(UbiSession ubiSession) {
    if (ubiSession.getEventCnt() >= MANY_EVENTS_BOUND) {
      return BotRules.MANY_EVENTS_BOT_FLAG;
    } else {
      return BotRules.NON_BOT_FLAG;
    }
  }
}
