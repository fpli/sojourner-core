package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotRules;

public class BotRule205 extends AbstractBotRule<UbiSession> {

  @Override
  public int getBotFlag(UbiSession session) {
    String sessionReferrer = session.getSessionReferrer();
    String ip = session.getExInternalIp();
    if (ip != null
        && ip.startsWith("10.")
        && sessionReferrer != null
        && (sessionReferrer.startsWith("http://cs.ebay.")
        || sessionReferrer.startsWith("https://cs.ebay."))) {
      return BotRules.CS_IP_BOTFLAG;
    }
    return 0;
  }
}
