package com.ebay.sojourner.business.rule;

import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.BotRules;
import com.ebay.sojourner.common.util.SOJStrBetweenEndList;

public class BotRule17 extends AbstractBotRule<UbiSession> {


  private static final String[] AKAMAI_BOTS = {"Request Anomaly", "HTTP Libraries",
      "Scraper Reputation", "Declared Bots", "Impersonators of Known Bots",
      "Site Monitoring and Web Development Bots", "RSS Feed Reader Bots", "SynAck (Bug-Bounty)",
      "Registration", "RSS Feed Reader Bots", "Web Search Engine Bots", "Online Advertising Bots",
      "Headless Browsers/Automation Tools", "Open Source Crawlers/Scraping Platforms",
      "Development Frameworks", "Social Media or Blog Bots", "SEO, Analytics or Marketing Bots",
      "Enterprise Data Aggregator Bots", "Web Services Libraries", "E-Commerce Search Engine Bots",
      "Web Archiver Bots", "Aggressive Web Crawlers", "News Aggregator Bots",
      "Academic or Research Bots"};

  @Override
  public int getBotFlag(UbiSession ubiSession) {
    String botProvider = ubiSession.getBotProvider();
    String akamaiStr = SOJStrBetweenEndList.strBetweenEndList(botProvider, "monitor:", "\"}");
    if (akamaiStr != null) {
      akamaiStr = akamaiStr.replace('+', ' ');
    }

    if (ubiSession.getBotFlagList().contains(BotRules.AKAMAI_BOT_FLAG)
        || (akamaiStr != null && matches(akamaiStr))) {
      return BotRules.AKAMAI_BOT_FLAG;
    } else {
      return BotRules.NON_BOT_FLAG;
    }
  }


  public boolean matches(String akamaiStr) {
    for (String str : AKAMAI_BOTS) {
      if (str.equals(akamaiStr)) {
        return true;
      }
    }
    return false;
  }
}
