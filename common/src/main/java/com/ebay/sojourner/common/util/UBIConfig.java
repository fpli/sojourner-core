package com.ebay.sojourner.common.util;

import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class UBIConfig {

  private static final Map<String, Object> confData = new ConcurrentHashMap<String, Object>();
  private static final String DEFAULT_FILE = "ubi.properties";
  private static Properties ubiProperties;

  static {
    InputStream resource = UBIConfig.class.getClassLoader().getResourceAsStream(DEFAULT_FILE);
    ubiProperties = initProperties(resource);
    try {
      initConfiguration(false);
      setConfiguration();
    } catch (Exception e) {
      log.error("Cannot init Configurations", e);
    }
  }

  private UBIConfig() {
    // private
  }

  protected static Properties initProperties(String filePath, String resource) {
    try {
      return PropertyUtils.loadInProperties(filePath, resource);
    } catch (Exception e) {
      log.error("Either unable to load resource either from " + filePath);
      log.error("Or unable to load from source from " + resource);
      throw new RuntimeException(e);
    }
  }

  private static Properties initProperties(InputStream filePath) {
    try {
      return PropertyUtils.loadInProperties(filePath);
    } catch (Exception e) {
      log.error("Unable to load resource", e);
      throw new RuntimeException(e);
    }
  }

  private static void setBoolean(String key, Boolean value) {
    Preconditions.checkNotNull(key, "Key must not be null.");
    Preconditions.checkNotNull(value, "Value must not be null.");

    synchronized (UBIConfig.class) {
      confData.put(key, value);
    }
  }

  protected static void setString(String key, String value) {
    Preconditions.checkNotNull(key, "Key must not be null.");
    Preconditions.checkNotNull(value, "Value must not be null.");

    synchronized (UBIConfig.class) {
      confData.put(key, value);
    }
  }

  private static void setLong(String key, Long value) {
    Preconditions.checkNotNull(key, "Key must not be null.");
    Preconditions.checkNotNull(value, "Value must not be null.");

    synchronized (UBIConfig.class) {
      confData.put(key, value);
    }
  }

  public static String getString(String key) {
    Object o = getRawValue(key);
    if (o == null) {
      return null;
    } else {
      return o.toString();
    }
  }

  public static Long getLong(String key) {
    Object o = getRawValue(key);
    if (o == null) {
      return null;
    } else {
      return Long.valueOf(o.toString());
    }
  }

  public static Boolean getBooleanOrDefault(String key, Boolean defaultBoolean) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultBoolean;
    } else {
      return Boolean.valueOf(o.toString());
    }
  }

  private static Object getRawValue(String key) {
    Preconditions.checkNotNull(key, "Key must not be null.");
    return confData.get(key);
  }

  private static void initConfiguration(boolean enableTest) throws Exception {

    if (getUBIProperty(Property.LOG_LEVEL) != null) {
      setString(Property.LOG_LEVEL, getUBIProperty(Property.LOG_LEVEL));
    }

    if (enableTest) {
      setBoolean(Property.IS_TEST_ENABLE, true);
    } else {
      setString(Property.IFRAME_PAGE_IDS, getUBIProperty(Property.IFRAME_PAGE_IDS));
      setString(Property.FINDING_FLAGS, getUBIProperty(Property.FINDING_FLAGS));
      setString(Property.VTNEW_IDS, getUBIProperty(Property.VTNEW_IDS));
      setString(Property.IAB_AGENT, getUBIProperty(Property.IAB_AGENT));
      setString(Property.APP_ID, getUBIProperty(Property.APP_ID));
      setString(Property.TEST_USER_IDS, getUBIProperty(Property.TEST_USER_IDS));
      setString(Property.LARGE_SESSION_GUID, getUBIProperty(Property.LARGE_SESSION_GUID));
      setString(Property.PAGE_FMLY, getUBIProperty(Property.PAGE_FMLY));
      setString(Property.MPX_ROTATION, getUBIProperty(Property.MPX_ROTATION));
      setString(Property.SELECTED_IPS, getUBIProperty(Property.SELECTED_IPS));
      setString(Property.SELECTED_AGENTS, getUBIProperty(Property.SELECTED_AGENTS));
      setString(Property.LKP_PATH,getUBIProperty(Property.LKP_PATH));
      setString(Property.PAGE_FMLY_ALL, getUBIProperty(Property.PAGE_FMLY_ALL));
      setString(Property.ITM_PAGES, getUBIProperty(Property.ITM_PAGES));
    }
  }

  private static void setConfiguration() throws Exception {
    // Set session properties
    setLong(
        Property.EVENT_WAITING_PERIOD,
        Long.parseLong(getUBIProperty(Property.EVENT_WAITING_PERIOD)) * Constants.MILLI2MICRO);
    setLong(
        Property.SESSION_IDLE_TIMEOUT,
        Long.parseLong(getUBIProperty(Property.SESSION_IDLE_TIMEOUT)) * Constants.MILLI2MICRO);
    setLong(
        Property.SESSION_MAX_DURATION,
        Long.parseLong(getUBIProperty(Property.SESSION_MAX_DURATION)) * Constants.MILLI2MICRO);
    setLong(
        Property.EVENT_DELAY_THRESHOLD,
        Long.parseLong(getUBIProperty(Property.EVENT_DELAY_THRESHOLD)) * Constants.MILLI2MICRO);
    // Set event properties
    setLong(
        Property.EVENT_POOL_THRESHOLD, Long.valueOf(getUBIProperty(Property.EVENT_POOL_THRESHOLD)));
    // Set Cobrand properties
    setString(Property.EXPRESS_SITE, getUBIProperty(Property.EXPRESS_SITE));
    setString(Property.HALF_SITE, getUBIProperty(Property.HALF_SITE));
    setString(Property.EXPRESS_PARTNER, getUBIProperty(Property.EXPRESS_PARTNER));
    setString(Property.SHOPPING_PARTNER, getUBIProperty(Property.SHOPPING_PARTNER));
    setString(Property.HALF_PARTNER, getUBIProperty(Property.HALF_PARTNER));
    setString(Property.ARTISAN_PARTNER, getUBIProperty(Property.ARTISAN_PARTNER));
    setString(Property.MOBILE_AGENT_START, getUBIProperty(Property.MOBILE_AGENT_START));
    setString(Property.MOBILE_AGENT_INDEX, getUBIProperty(Property.MOBILE_AGENT_INDEX));
    setString(Property.MOBILE_AGENT_OTHER, getUBIProperty(Property.MOBILE_AGENT_OTHER));
    // Set page indicators
    setString(Property.SEARCH_VIEW_PAGES, getUBIProperty(Property.SEARCH_VIEW_PAGES));
    setString(Property.VIEW_ITEM_PAGES, getUBIProperty(Property.VIEW_ITEM_PAGES));
    setString(Property.BID_PAGES, getUBIProperty(Property.BID_PAGES));
    setString(Property.BIN_PAGES, getUBIProperty(Property.BIN_PAGES));
    setString(Property.CAPTCHA_PAGES, getUBIProperty(Property.CAPTCHA_PAGES));
    setString(Property.HALF_PAGES, getUBIProperty(Property.HALF_PAGES));
    setString(Property.CORESITE_PAGES, getUBIProperty(Property.CORESITE_PAGES));
    setString(Property.CLASSIFIED_PAGES, getUBIProperty(Property.CLASSIFIED_PAGES));
    // New metrics need page list
    setString(Property.ROVER_PAGES, getUBIProperty(Property.ROVER_PAGES));
    setString(Property.LAND_PAGES1, getUBIProperty(Property.LAND_PAGES1));
    setString(Property.LAND_PAGES2, getUBIProperty(Property.LAND_PAGES2));
    setString(Property.SCEVENT_EXCLUDE_PAGES1, getUBIProperty(Property.SCEVENT_EXCLUDE_PAGES1));
    setString(Property.SCEVENT_EXCLUDE_PAGES2, getUBIProperty(Property.SCEVENT_EXCLUDE_PAGES2));
    setString(Property.AGENT_EXCLUDE_PAGES, getUBIProperty(Property.AGENT_EXCLUDE_PAGES));
    setString(Property.NOTIFY_CLICK_PAGES, getUBIProperty(Property.NOTIFY_CLICK_PAGES));
    setString(Property.NOTIFY_VIEW_PAGES, getUBIProperty(Property.NOTIFY_VIEW_PAGES));
    setString(Property.LNDG_PAGE_IDS, getUBIProperty(Property.LNDG_PAGE_IDS));
    setString(Property.IP_EXCLUDE_PAGES, getUBIProperty(Property.IP_EXCLUDE_PAGES));
    setString(Property.EXCLUDE_IP_PATTERN, getUBIProperty(Property.EXCLUDE_IP_PATTERN));
    setString(Property.EBAY_SITE_COBRAND, getUBIProperty(Property.EBAY_SITE_COBRAND));
    setString(Property.INVALID_BOT_FILTER, getUBIProperty(Property.INVALID_BOT_FILTER));
    setString(Property.MOBILE_PAGES, getUBIProperty(Property.MOBILE_PAGES));
    setString(Property.PRELOAD_PAYLOAD_TAGS, getUBIProperty(Property.PRELOAD_PAYLOAD_TAGS));

    // Extra metrics for page list
    setString(Property.INVALID_PAGE_IDS, getUBIProperty(Property.INVALID_PAGE_IDS));
    // Set APP PAYLOAD KV Property
    setString(Property.SWD_VALUES, getUBIProperty(Property.SWD_VALUES));
    setString(Property.ROT_VALUES, getUBIProperty(Property.ROT_VALUES));
    setString(Property.SOCIAL_AGENT_ID22, getUBIProperty(Property.SOCIAL_AGENT_ID22));
    setString(Property.SOCIAL_AGENT_ID23, getUBIProperty(Property.SOCIAL_AGENT_ID23));
    setString(Property.VI_EVENT_VALUES, getUBIProperty(Property.VI_EVENT_VALUES));
    // Set APP ID property
    setString(Property.MOBILE_APP, getUBIProperty(Property.MOBILE_APP));
    setString(Property.DESKTOP_APP, getUBIProperty(Property.DESKTOP_APP));
    setString(Property.EIM_APP, getUBIProperty(Property.EIM_APP));
    // Set BOT rules
    setString(Property.BOT_RULE_PACKAGE, getUBIProperty(Property.BOT_RULE_PACKAGE));
    setString(Property.BOT_RULE_CLASSES, getUBIProperty(Property.BOT_RULE_CLASSES));
    // Set large session properties
    setLong(
        Property.LARGE_SESSION_EVENT_NUMBER,
        Long.valueOf(getUBIProperty(Property.LARGE_SESSION_EVENT_NUMBER)));
    setLong(
        Property.LARGE_SESSION_TIMES_OF_BOT15,
        Long.valueOf(getUBIProperty(Property.LARGE_SESSION_TIMES_OF_BOT15)));
    // Set disabled filters
    String disabledFilterNames = getUBIProperty(Property.DISABLED_FILTER_NAMES);
    if (StringUtils.isNotBlank(disabledFilterNames)) {
      setString(Property.DISABLED_FILTER_NAMES, disabledFilterNames);
    }
  }

  public static String getUBIProperty(String property) {
    return ubiProperties.getProperty(property);
  }
}
