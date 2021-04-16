package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.PropertyUtils;
import com.ebay.sojourner.common.util.SOJListGetValueByIndex;
import com.ebay.sojourner.common.util.SojEventTimeUtil;
import com.ebay.sojourner.common.util.UBIConfig;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentIPMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  private Set<Integer> badIPPages = new HashSet<>();
  private Pattern invalidIPPattern;

  @Override
  public void init() throws Exception {
    badIPPages = PropertyUtils.getIntegerSet(
            UBIConfig.getString(Property.IP_EXCLUDE_PAGES), Property.PROPERTY_DELIMITER);
    log.info("UBIConfig.getString(Property.IP_EXCLUDE_PAGES): {}",
        UBIConfig.getString(Property.IP_EXCLUDE_PAGES));
    String patternStr = UBIConfig.getString(Property.EXCLUDE_IP_PATTERN);
    invalidIPPattern = Pattern.compile(patternStr);
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) {
    sessionAccumulator.getUbiSession().setFindFirst(false);
    sessionAccumulator.getUbiSession().setInternalIp(null);
    sessionAccumulator.getUbiSession().setExternalIp(null);
    sessionAccumulator.getUbiSession().setExternalIp2(null);
    sessionAccumulator.getUbiSession().setAgentInfo(null);
    sessionAccumulator.getUbiSession().setClientIp(null);
    sessionAccumulator.getUbiSession().setBrowserFamily(null);
    sessionAccumulator.getUbiSession().setBrowserVersion(null);
    sessionAccumulator.getUbiSession().setDeviceFamily(null);
    sessionAccumulator.getUbiSession().setDeviceClass(null);
    sessionAccumulator.getUbiSession().setOsFamily(null);
    sessionAccumulator.getUbiSession().setOsVersion(null);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
    UbiSession ubiSession = sessionAccumulator.getUbiSession();
    // change agent info whenever an event feeds
    ubiSession.setAgentInfo(event.getAgentInfo());

    boolean isEarlyEvent = SojEventTimeUtil.isEarlyEvent(event.getEventTimestamp(),
            ubiSession.getAbsStartTimestamp());
    boolean isEarlyEventByMultiCols = SojEventTimeUtil.isEarlyByMultiCOls(event, ubiSession);
    boolean isEarlyValidEvent = SojEventTimeUtil.isEarlyEvent(event.getEventTimestamp(),
            sessionAccumulator.getUbiSession().getStartTimestamp());
    boolean isEarlyNoIframeEvent = SojEventTimeUtil.isEarlyEvent(event.getEventTimestamp(),
            sessionAccumulator.getUbiSession().getStartTimestampNOIFRAME());
    if (isEarlyEvent) {
      if (!ubiSession.isFindFirst()) {
        ubiSession.setUserAgent(event.getAgentInfo());
        ubiSession.setClientIp(event.getClientIP());
        setDeviceMetrics(ubiSession, event);
      }
    } else if (isEarlyEventByMultiCols) {
      if (!ubiSession.isFindFirst()) {
        ubiSession.setUserAgent(event.getAgentInfo());
        ubiSession.setClientIp(event.getClientIP());
        setDeviceMetrics(ubiSession, event);
      }
    }

    if (isEarlyValidEvent) {
      if (!event.isIframe() && !event.isRdt()) {
        ubiSession.setUserAgent(event.getAgentInfo());
        ubiSession.setClientIp(event.getClientIP());
        setDeviceMetrics(ubiSession, event);
        ubiSession.setFindFirst(true);
      }
    }

    // to avoid the cut off issue on 2018-02-09
    if (event.isPartialValidPage()) {
      if (!event.isIframe() && !event.isRdt()) {
        String remoteIp = event.getClientData().getRemoteIP();
        String forwardFor = event.getClientData().getForwardFor();
        if (ubiSession.getExternalIp() == null) {
          ubiSession.setExternalIp(getExternalIP(event, remoteIp, forwardFor));
          if (ubiSession.getExternalIp() == null && ubiSession.getInternalIp() == null) {
            ubiSession.setInternalIp(getInternalIP(remoteIp, forwardFor));
          }
        } else if (isEarlyValidEvent) {
          String externalIp = getExternalIP(event, remoteIp, forwardFor);
          if (externalIp != null) {
            ubiSession.setExternalIp(externalIp);
            ubiSession.setInternalIp(null);
          } else {
            String internalIp = getInternalIP(remoteIp, forwardFor);
            if (internalIp != null) {
              ubiSession.setInternalIp(internalIp);
            }
          }
        }
      }
    }

    if (!event.isIframe()) {
      String remoteIp = event.getClientData().getRemoteIP();
      String forwardFor = event.getClientData().getForwardFor();
      if (ubiSession.getExternalIp2() == null) {
        ubiSession.setExternalIp2(getExternalIP(event, remoteIp, forwardFor));
      } else if (isEarlyNoIframeEvent) {
        String externalIp2 = getExternalIP(event, remoteIp, forwardFor);
        if (externalIp2 != null) {
          ubiSession.setExternalIp2(externalIp2);
        }
      }
    }
  }

  private void setDeviceMetrics(UbiSession ubiSession, UbiEvent event) {
    ubiSession.setBrowserFamily(event.getBrowserFamily());
    ubiSession.setBrowserVersion(event.getBrowserVersion());
    ubiSession.setDeviceFamily(event.getDeviceFamily());
    ubiSession.setDeviceClass(event.getDeviceType());
    ubiSession.setOsFamily(event.getOsFamily());
    ubiSession.setOsVersion(event.getEnrichedOsVersion());
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) {
    // change the logic to align with caleb's on 2018-02-06
    //  exInternalIp = externalIp == null ? internalIp : externalIp;

    sessionAccumulator.getUbiSession()
        .setUserAgent(sessionAccumulator.getUbiSession().getUserAgent());
    sessionAccumulator.getUbiSession().setIp(sessionAccumulator.getUbiSession().getClientIp());
    sessionAccumulator.getUbiSession().setExInternalIp(
        (sessionAccumulator.getUbiSession().getExternalIp() == null) ? (
            sessionAccumulator.getUbiSession().getExternalIp2() == null ? sessionAccumulator
                .getUbiSession().getInternalIp()
                : sessionAccumulator.getUbiSession().getExternalIp2())
            : sessionAccumulator.getUbiSession().getExternalIp());
  }

  public String getExternalIP(UbiEvent event, String remoteIp, String forwardFor) {
    int pageId = event.getPageId();
    String urlQueryString = event.getUrlQueryString();
    if (badIPPages.contains(pageId)) {
      return null;
    }
    if (pageId == 3686 && urlQueryString != null && urlQueryString.contains("Portlet")) {
      return null;
    }

    if (remoteIp != null && !(invalidIPPattern.matcher(remoteIp).matches())) {
      return remoteIp;
    }

    for (int i = 1; i < 4; i++) {
      String forwardValueByIndex = SOJListGetValueByIndex.getValueByIndex(forwardFor, ",", i);
      if (forwardValueByIndex != null
          && !(invalidIPPattern.matcher(forwardValueByIndex).matches())) {
        return forwardValueByIndex;
      }
    }
    return null;
  }

  public String getInternalIP(String remoteIp, String forwardFor) {
    if (remoteIp != null) {
      return remoteIp;
    }
    return SOJListGetValueByIndex.getValueByIndex(forwardFor, ",", 1);
  }
}
