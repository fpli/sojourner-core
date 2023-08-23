package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotRules;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class BotRuleForIosHpBot extends AbstractBotRule<UbiSession> {
  private static final List<Integer> IOS_APP_IDS = Arrays.asList(1462, 2878);
  private static final List<Integer> IOS_HOME_PAGE_IDS = Arrays.asList(
          2481888, 4375194, 4445145
  );
  private static final int NOTIFICATION_HUB_PAGE_ID = 2380424;

  @Override
  public int getBotFlag(UbiSession ubiSession) {
    if (ubiSession.getBotFlagList().contains(BotRules.IOS_HP_BOT)
        || (matches(ubiSession))) {
      return BotRules.IOS_HP_BOT;
    } else {
      return BotRules.NON_BOT_FLAG;
    }
  }

  public boolean matches(UbiSession ubiSession) {
    return (ubiSession.getCobrand() == 6 && IOS_APP_IDS.contains(ubiSession.getAppId())
        && IOS_HOME_PAGE_IDS.contains(ubiSession.getLndgPageIdForIos())
        && ubiSession.getExitPageIdForIos() == ubiSession.getLndgPageIdForIos()
        && ubiSession.getIsExistForegroundEvent() == 0
        && ubiSession.getIsExistBackgroundEvent() == 0
        && ubiSession.getValidPageCntForIos() <= 2)
      || (ubiSession.getCobrand() == 6 && IOS_APP_IDS.contains(ubiSession.getAppId())
        && StringUtils.isEmpty(ubiSession.getIdfa())
        && IOS_HOME_PAGE_IDS.contains(ubiSession.getLndgPageIdForIos())
        && ubiSession.getExitPageIdForIos() == ubiSession.getLndgPageIdForIos()
        && ubiSession.getValidPageCntForIos() <= 5)
      || (ubiSession.getCobrand() == 6 && IOS_APP_IDS.contains(ubiSession.getAppId())
      && StringUtils.isEmpty(ubiSession.getIdfa())
      && ubiSession.getLndgPageIdForIos() == NOTIFICATION_HUB_PAGE_ID
      && ubiSession.getIsExistForegroundEvent() == 0
      && ubiSession.getIsExistBackgroundEvent() == 0)
    ;
  }
}
