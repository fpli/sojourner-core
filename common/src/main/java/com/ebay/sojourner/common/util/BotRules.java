package com.ebay.sojourner.common.util;

public class BotRules {

  // Intraday BOT Flag
  public static final int NON_BOT_FLAG = 0;
  public static final int SPIDER_BOT_FLAG = 1;
  public static final int FAST_EVENTS_BOT_FLAG = 3;
  public static final int REGULAR_EVENTS_BOT_FLAG = 4;
  public static final int MANY_SEARCH_VIEW_BOT_FLAG = 9;
  public static final int AUTO_CAPTCHA_BOT_FLAG = 10;
  public static final int MANY_FAST_EVENTS_BOT_FLAG = 12;
  public static final int MANY_EVENTS_BOT_FLAG = 15;
  public static final int AKAMAI_BOT_FLAG = 17;


  // Intraday New BOT Flag
  public static final int MANY_VIEW_WITHOUT_SIID = 203;
  public static final int MANY_SRP_WITHOUT_SIID = 204;
  public static final int CS_IP_BOTFLAG = 205;
  public static final int SHORT_SESSION_WITHOUT_AGENT = 206;
  public static final int MANY_VALID_EVENTS_WHITHOUT_REFERER = 207;
  public static final int DIRECT_ACCESS_BOTFLAG = 208;
  public static final int HIGH_DENSITY_VIEWS = 215;
  public static final int MANY_VALID_PAGE = 212;
  // EOD BOT Flag
  public static final int SCS_ON_AGENTIP = 5;
  public static final int SCS_ON_AGENT = 6;
  public static final int SCS_ON_IP = 7;
  public static final int SCS_CONFIRM_ON_AGENTIP = 8;
  public static final int SPECIFIC_SPIDER_IAB = 11;
  public static final int FAST_REGULAR_AGENTIP_EXTEND = 102;
  public static final int FAST_EVENTS_EXTEND_AGENTIP = 103;
  public static final int REGULAR_EVENTS_EXTEND_AGENTIP = 104;
  public static final int MANY_EVENTS_EXTEND_EOD = 15;
  // EOD New BOT Flag
  public static final int SUSPECTED_IP_ON_AGENT = 210;
  public static final int DECLARED_AGENT = 202;
  public static final int SAME_AGENT_IP = 211;
  // ICF BOT Flag (801 ~ 856 represent 1 ~ 56)
  public static final int ICF_PREFIX = 8;
  // userid new rule
  public static final int BLACK_USER_ID = 16;

}
