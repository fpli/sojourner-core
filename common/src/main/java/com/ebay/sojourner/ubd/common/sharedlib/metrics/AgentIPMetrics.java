package com.ebay.sojourner.ubd.common.sharedlib.metrics;


import com.ebay.sojourner.ubd.common.model.SessionAccumulator;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.model.UbiSession;
import com.ebay.sojourner.ubd.common.sharedlib.util.SOJListGetValueByIndex;
import com.ebay.sojourner.ubd.common.util.Property;
import com.ebay.sojourner.ubd.common.util.PropertyUtils;
import com.ebay.sojourner.ubd.common.util.UBIConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public class AgentIPMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

    private Set<Integer> badIPPages;
    private Pattern invalidIPPattern;

    @Override
    public void init() throws Exception {
        badIPPages = PropertyUtils.getIntegerSet(UBIConfig.getString(Property.IP_EXCLUDE_PAGES), Property.PROPERTY_DELIMITER);
        log.info("UBIConfig.getString(Property.IP_EXCLUDE_PAGES): {}", UBIConfig.getString(Property.IP_EXCLUDE_PAGES));
        String patternStr = UBIConfig.getString(Property.EXCLUDE_IP_PATTERN);
        invalidIPPattern = Pattern.compile(patternStr);
    }

    @Override
    public void start(SessionAccumulator sessionAccumulator) {
        sessionAccumulator.getUbiSession().setFindFirst(false);
        sessionAccumulator.getUbiSession().setInternalIp(null);
        sessionAccumulator.getUbiSession().setExternalIp(null);
        sessionAccumulator.getUbiSession().setExternalIp2(null);
    }

    @Override
    public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) {
        UbiSession ubiSession = sessionAccumulator.getUbiSession();

        if (!ubiSession.isFindFirst() && event.getClientIP() != null) {
            ubiSession.setAgentInfo(event.getAgentInfo());
            ubiSession.setClientIp(event.getClientIP());
        }

        if (!event.isIframe() && !event.isRdt() && !ubiSession.isFindFirst()) {
            ubiSession.setAgentInfo(event.getAgentInfo());
            ubiSession.setClientIp(event.getClientIP());
            ubiSession.setFindFirst(true);
        }
        // to avoid the cut off issue on 2018-02-09
        if (event.isPartialValidPage()) {
            if (!event.isIframe() && !event.isRdt() && ubiSession.getExternalIp() == null) {
                String remoteIp = event.getClientData().getRemoteIP(); //SOJParseClientInfo.getClientInfo(event.getClientData(), "RemoteIP");
                String forwardFor = event.getClientData().getForwardFor();// SOJParseClientInfo.getClientInfo(event.getClientData(), "ForwardedFor");
                ubiSession.setExternalIp(getExternalIP(event, remoteIp, forwardFor));
                if (ubiSession.getExternalIp() == null && ubiSession.getInternalIp() == null) {
                    ubiSession.setInternalIp(getInternalIP(remoteIp, forwardFor));
                }
            }
        }

        if (!event.isIframe()) {
            if (ubiSession.getExternalIp2() == null) {
                String remoteIp = event.getClientData().getRemoteIP(); //SOJParseClientInfo.getClientInfo(event.getClientData(), "RemoteIP");
                String forwardFor = event.getClientData().getForwardFor();// SOJParseClientInfo.getClientInfo(event.getClientData(), "ForwardedFor");
                ubiSession.setExternalIp2(getExternalIP(event, remoteIp, forwardFor));
            }
        }
    }

    @Override
    public void end(SessionAccumulator sessionAccumulator) {
        //change the logic to align with caleb's on 2018-02-06
        //  exInternalIp = externalIp == null ? internalIp : externalIp;

        sessionAccumulator.getUbiSession().setUserAgent(sessionAccumulator.getUbiSession().getAgentInfo());
        sessionAccumulator.getUbiSession().setIp(sessionAccumulator.getUbiSession().getClientIp());
        sessionAccumulator.getUbiSession().setExInternalIp((sessionAccumulator.getUbiSession().getExternalIp() == null) ?
                (sessionAccumulator.getUbiSession().getExternalIp2() == null ? sessionAccumulator.getUbiSession().getInternalIp() : sessionAccumulator.getUbiSession().getExternalIp2()) : sessionAccumulator.getUbiSession().getExternalIp());
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
            if (forwardValueByIndex != null && !(invalidIPPattern.matcher(forwardValueByIndex).matches())) {
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
