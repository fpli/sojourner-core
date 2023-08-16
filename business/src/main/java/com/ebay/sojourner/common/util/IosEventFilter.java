package com.ebay.sojourner.common.util;

import com.ebay.sojourner.common.model.UbiEvent;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class IosEventFilter {

  private static Set<Integer> invalidPageIds;
  public static final Pattern IOS_UA_PATTERN = Pattern.compile(".*eBay(iPhone|IOS|iPad).*");
  private static final int NOTIFICATION_HUB_PAGE_ID = 2380424;
  private static final int SRP_PAGE_ID = 2351460;

  public static boolean isIosEvent(String agent) {
    return IOS_UA_PATTERN.matcher(agent).matches();
  }

  public static boolean isValidIosEvent(UbiEvent event) {
    if (!isIosEvent(event.getAgentInfo())) return false;

    // Notification Hub on IOS Native Apps
    String app = SOJNVL.getTagValue(event.getApplicationPayload(), "app");
    boolean appFlag = Objects.equals(app, "1462") || Objects.equals(app, "2878");
    if (event.getPageId() == NOTIFICATION_HUB_PAGE_ID && appFlag) {
      return false;
    }

    // IOS Native App Experience Services Search without sHit logging
    String sHit = SOJNVL.getTagValue(event.getApplicationPayload(), "sHit");
    if (event.getPageId() == SRP_PAGE_ID && StringUtils.isEmpty(sHit) && appFlag) {
      return false;
    }

    invalidPageIds = PropertyUtils.getIntegerSet(
        UBIConfig.getString(Property.INVALID_PAGE_IDS), Property.PROPERTY_DELIMITER);
    int csTracking = 0;
    if (StringUtils.isNotBlank(event.getUrlQueryString())
        && (event.getUrlQueryString().startsWith("/roverimp")
        || event.getUrlQueryString().contains("SojPageView"))) {
      csTracking = 1;
    }
    if (event.isPartialValidPage()
        && !event.isIframe()
        && ((event.getPageId() != -1 && !invalidPageIds.contains(event.getPageId()))
        || csTracking == 0)) {
      return true;
    }
    return false;
  }

  public static boolean isHpOrFgOrLaunchIosEvent(UbiEvent event) {
    List<Integer> HP_LAUNCH_PAGE_IDS = Arrays.asList(2051248,2051249,2367320,2481888,3562572);
    String app = SOJNVL.getTagValue(event.getApplicationPayload(), "app");

    return HP_LAUNCH_PAGE_IDS.contains(event.getPageId())
        && (Objects.equals(app, "1462") || Objects.equals(app, "2878"));
  }

}
