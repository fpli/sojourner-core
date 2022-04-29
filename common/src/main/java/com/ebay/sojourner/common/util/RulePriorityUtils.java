package com.ebay.sojourner.common.util;

import java.util.Set;
import org.apache.commons.collections.CollectionUtils;

public class RulePriorityUtils {

  public static int getHighPriorityBotFlag(Set<Integer> botFlags) {

    if (CollectionUtils.isEmpty(botFlags)) {
      return BotRules.NON_BOT_FLAG;
    }

    if (botFlags.contains(BotRules.BLACK_USER_ID)) {
      return BotRules.BLACK_USER_ID;
    } else if (botFlags.contains(BotRules.MANY_EVENTS_BOT_FLAG)) {
      return BotRules.MANY_EVENTS_BOT_FLAG;
    } else if (botFlags.contains(BotRules.SPIDER_BOT_FLAG)) {
      return BotRules.SPIDER_BOT_FLAG;
    } else if (botFlags.contains(BotRules.MANY_SEARCH_VIEW_BOT_FLAG)) {
      return BotRules.MANY_SEARCH_VIEW_BOT_FLAG;
    } else if (botFlags.contains(BotRules.AUTO_CAPTCHA_BOT_FLAG)) {
      return BotRules.AUTO_CAPTCHA_BOT_FLAG;
    } else if (botFlags.contains(BotRules.MANY_FAST_EVENTS_BOT_FLAG)) {
      return BotRules.MANY_FAST_EVENTS_BOT_FLAG;
    } else if (botFlags.contains(BotRules.MANY_VIEW_WITHOUT_SIID)) {
      return BotRules.MANY_VIEW_WITHOUT_SIID;
    } else if (botFlags.contains(BotRules.MANY_SRP_WITHOUT_SIID)) {
      return BotRules.MANY_SRP_WITHOUT_SIID;
    } else if (botFlags.contains(BotRules.CS_IP_BOTFLAG)) {
      return BotRules.CS_IP_BOTFLAG;
    } else if (botFlags.contains(BotRules.SHORT_SESSION_WITHOUT_AGENT)) {
      return BotRules.SHORT_SESSION_WITHOUT_AGENT;
    } else if (botFlags.contains(BotRules.MANY_VALID_EVENTS_WHITHOUT_REFERER)) {
      return BotRules.MANY_VALID_EVENTS_WHITHOUT_REFERER;
    } else if (botFlags.contains(BotRules.DIRECT_ACCESS_BOTFLAG)) {
      return BotRules.DIRECT_ACCESS_BOTFLAG;
    } else if (botFlags.contains(BotRules.MANY_VALID_PAGE)) {
      return BotRules.MANY_VALID_PAGE;
    } else if (botFlags.contains(BotRules.HIGH_DENSITY_VIEWS)) {
      return BotRules.HIGH_DENSITY_VIEWS;
    }else if (botFlags.contains(BotRules.AKAMAI_BOT_FLAG)) {
      return BotRules.AKAMAI_BOT_FLAG;
    }else if (botFlags.contains(BotRules.AGENT_HOPPING_BOT_FLAG)) {
      return BotRules.AGENT_HOPPING_BOT_FLAG;
    } else if (botFlags.contains(BotRules.SPECIFIC_SPIDER_IAB)) {
      return BotRules.SPECIFIC_SPIDER_IAB;
    } else if (botFlags.contains(BotRules.SCS_CONFIRM_ON_AGENTIP)) {
      return BotRules.SCS_CONFIRM_ON_AGENTIP;
    } else if (botFlags.contains(BotRules.SCS_ON_AGENTIP)) {
      return BotRules.SCS_ON_AGENTIP;
    } else if (botFlags.contains(BotRules.SCS_ON_AGENT)) {
      return BotRules.SCS_ON_AGENT;
    } else if (botFlags.contains(BotRules.SCS_ON_IP)) {
      return BotRules.SCS_ON_IP;
    } else if (botFlags.contains(BotRules.FAST_REGULAR_AGENTIP_EXTEND)) {
      return BotRules.FAST_REGULAR_AGENTIP_EXTEND;
    } else if (botFlags.contains(BotRules.DECLARED_AGENT)) {
      return BotRules.DECLARED_AGENT;
    } else if (botFlags.contains(BotRules.SUSPECTED_IP_ON_AGENT)) {
      return BotRules.SUSPECTED_IP_ON_AGENT;
    } else if (botFlags.contains(BotRules.SAME_AGENT_IP)) {
      return BotRules.SAME_AGENT_IP;
    }

    return BotRules.NON_BOT_FLAG;
  }

}
