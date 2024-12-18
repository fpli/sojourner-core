package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotRules;

public class BotRule203 extends AbstractBotRule<UbiSession> {

  @Override
  public int getBotFlag(UbiSession session) {
    if (session.getValidPageCnt() == session.getFamilyViCnt() && session.getSiidCnt() == 0) {
      if (session.getValidPageCnt() > 20
          && (session.getFirstSiteId() == Integer.MIN_VALUE || session.getFirstSiteId() != 100)) {
        return BotRules.MANY_VIEW_WITHOUT_SIID;
      }
      if (session.getValidPageCnt() > 100
          && session.getFirstSiteId() != Integer.MIN_VALUE
          && session.getFirstSiteId() == 100) {
        return BotRules.MANY_VIEW_WITHOUT_SIID;
      }
    }
    return 0;
  }
}
