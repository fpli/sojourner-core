package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.SOJNVL;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

@Slf4j
public class AppIdParser implements FieldParser<RawEvent, UbiEvent> {

  private static final String APPID = "app";
  private static Pattern pattern = Pattern.compile("ebayUserAgent/eBayIOS;.*;iPad.*", 2);
  private static Pattern pattern1 = Pattern.compile("ebayUserAgent/eBayIOS;.*", 2);
  private static Pattern pattern2 = Pattern.compile("ebayUserAgent/eBayAndroid;.*", 2);
  private static Pattern pattern3 = Pattern.compile("eBayiPad/.*", 2);
  private static Pattern pattern4 = Pattern.compile("eBayiPhone/.*", 2);
  private static Pattern pattern5 = Pattern.compile("eBayAndroid/.*", 2);
  private static Pattern pattern6 = Pattern.compile("iphone/5.*", 2);
  private static Pattern pattern7 = Pattern.compile("Android/5.*", 2);

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws RuntimeException {
    String appid = null;
    Map<String, String> map = new HashMap<>();
    map.putAll(rawEvent.getSojA());
    map.putAll(rawEvent.getSojK());
    map.putAll(rawEvent.getSojC());
    String applicationPayload = ubiEvent.getApplicationPayload();
    String agentInfo = rawEvent.getClientData().getAgent() == null ? ""
        : rawEvent.getClientData().getAgent();
    if (pattern.matcher(agentInfo).matches()) {
      appid = "2878";
    } else if (pattern1.matcher(agentInfo).matches()) {
      appid = "1462";
    } else if (pattern2.matcher(agentInfo).matches()) {
      appid = "2571";
    } else if (pattern3.matcher(agentInfo).matches()) {
      appid = "2878";
    } else if (pattern4.matcher(agentInfo).matches()) {
      appid = "1462";
    } else if (pattern5.matcher(agentInfo).matches()) {
      appid = "2571";
    } else if (pattern6.matcher(agentInfo).matches()) {
      appid = "1462";
    } else if (pattern7.matcher(agentInfo).matches()) {
      appid = "2571";
    } else if (SOJNVL.getTagValue(applicationPayload, APPID) != null
        && NumberUtils.isNumber(SOJNVL.getTagValue(applicationPayload, APPID))) {
      appid = SOJNVL.getTagValue(applicationPayload, APPID);
    }
    try {
      if (StringUtils.isNotBlank(appid)) {
        ubiEvent.setAppId(Integer.parseInt(appid));
      }
    } catch (NumberFormatException e) {
      // log.warn("Parsing appId failed, format incorrect: " + appid);
    }
  }

  @Override
  public void init() throws Exception {
  }
}
