package com.ebay.sojourner.common.util;

import com.ebay.sojourner.common.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class SojUtils {
    private static final String SPLIT_DEL = "\\|";
    public static boolean isRover3084Click(UbiEvent event) {
        if (event.getPageId() == -1) {
            return false;
        }
        return 3084 == event.getPageId();
    }

    public static boolean isRover3085Click(UbiEvent event) {
        if (event.getPageId() == -1) {
            return false;
        }
        return event.getPageId() == 3085;
    }

    public static boolean isRover3962Click(UbiEvent event) {
        if (event.getPageId() == -1) {
            return false;
        }
        return event.getPageId() == 3962;
    }

    public static boolean isRoverClick(UbiEvent event) {
        return IntermediateLkp.getInstance().getRoverPageSet().contains(event.getPageId());
    }

    public static boolean isScEvent(UbiEvent event) {
        Integer pageId = event.getPageId() == -1 ? -99 : event.getPageId();
        return !event.isRdt()
                && !event.isIframe()
                // || urlQueryString.matches("(/roverimp|.*SojPageView).*")
                && !IntermediateLkp.getInstance().getScPageSet1().contains(pageId)
                && !IntermediateLkp.getInstance().getScPageSet2().contains(pageId);
    }

    public static SojEvent convertUbiEvent2SojEvent(UbiEvent ubiEvent) {
        SojEvent sojEvent = new SojEvent();
        sojEvent.setGuid(ubiEvent.getGuid());
        sojEvent.setAppId(ubiEvent.getAppId() == null ? null :
                String.valueOf(ubiEvent.getAppId()));
        sojEvent.setApplicationPayload(
            PropertyUtils.stringToMap(ubiEvent.getApplicationPayload(), false));
        sojEvent.setAppVersion(ubiEvent.getAppVersion());
        sojEvent.setBotFlags(new ArrayList<>(ubiEvent.getBotFlags()));
        sojEvent.setClientData(
                ubiEvent.getClientData() == null ? null :
                        PropertyUtils.stringToMap(
                                ubiEvent.getClientData().toString(), true));
        sojEvent.setBrowserFamily(ubiEvent.getBrowserFamily());
        sojEvent.setBrowserVersion(ubiEvent.getBrowserVersion());
        sojEvent.setClickId(ubiEvent.getClickId() == -1 ? null :
                String.valueOf(ubiEvent.getClickId()));
        sojEvent.setClientIP(ubiEvent.getClientIP());
        sojEvent.setCobrand(String.valueOf(ubiEvent.getCobrand()));
        sojEvent.setCookies(ubiEvent.getCookies());
        sojEvent.setCurrentImprId(ubiEvent.getCurrentImprId());
        sojEvent.setDataCenter(ubiEvent.getDataCenter());
        sojEvent.setDeviceFamily(ubiEvent.getDeviceFamily());
        sojEvent.setDeviceType(ubiEvent.getDeviceType());
        sojEvent.setEnrichedOsVersion(ubiEvent.getEnrichedOsVersion());
        sojEvent.setEventAction(ubiEvent.getEventAction());
        sojEvent.setEventCaptureTime(ubiEvent.getEventCaptureTime());
        sojEvent.setEventAttr(ubiEvent.getEventAttr());
        sojEvent.setEventCnt(ubiEvent.getEventCnt());
        sojEvent.setEventFamily(ubiEvent.getEventFamily());
        sojEvent.setEventTimestamp(ubiEvent.getEventTimestamp() != null ?
                SojTimestamp.getUnixTimestamp(ubiEvent.getEventTimestamp()) : null);
        sojEvent.setFlags(ubiEvent.getFlags());
        sojEvent.setForwardedFor(ubiEvent.getForwardedFor());
        sojEvent.setGenerateTime(ubiEvent.getGenerateTime());
        sojEvent.setIcfBinary(ubiEvent.getIcfBinary());
        sojEvent.setIframe(ubiEvent.isIframe());
        sojEvent.setIngestTime(ubiEvent.getIngestTime());
        sojEvent.setItemId(ubiEvent.getItemId() == null ? null
                : String.valueOf(ubiEvent.getItemId()));
        sojEvent.setOldSessionSkey(ubiEvent.getOldSessionSkey());
        sojEvent.setOsFamily(ubiEvent.getOsFamily());
        sojEvent.setOsVersion(ubiEvent.getOsVersion());
        sojEvent.setPageFamily(ubiEvent.getPageFamily());
        sojEvent.setPageId(ubiEvent.getPageId() == -1 ? null : ubiEvent.getPageId());
        sojEvent.setPageName(ubiEvent.getPageName());
        sojEvent.setAgentInfo(ubiEvent.getAgentInfo());
        sojEvent.setPartialValidPage(ubiEvent.isPartialValidPage());
        sojEvent.setRdt(ubiEvent.isRdt() ? 1 : 0);
        sojEvent.setRefererHash(
                ubiEvent.getRefererHash() == null ? null
                        : String.valueOf(ubiEvent.getRefererHash()));
        sojEvent.setReferrer(ubiEvent.getReferrer());
        sojEvent.setRegu(ubiEvent.getRegu());
        sojEvent.setRemoteIP(ubiEvent.getRemoteIP());
        sojEvent.setRequestCorrelationId(ubiEvent.getRequestCorrelationId());
        sojEvent.setReservedForFuture(ubiEvent.getReservedForFuture());
        sojEvent.setRlogid(ubiEvent.getRlogid());
        sojEvent.setSeqNum(String.valueOf(ubiEvent.getSeqNum()));
        sojEvent.setSessionEndTime(ubiEvent.getSessionEndTime());
        sojEvent.setSessionSkey(ubiEvent.getSessionSkey());
        sojEvent.setSessionId(ubiEvent.getSessionId());
        sojEvent.setSessionStartDt(ubiEvent.getSessionStartDt());
        sojEvent.setSojDataDt(ubiEvent.getSojDataDt());
        sojEvent.setSessionStartTime(ubiEvent.getSessionStartTime());
        sojEvent.setSid(ubiEvent.getSid());
        sojEvent.setSiteId(ubiEvent.getSiteId() == -1 ? null
                : String.valueOf(ubiEvent.getSiteId()));
        sojEvent.setSourceImprId(ubiEvent.getSourceImprId());
        sojEvent.setSqr(ubiEvent.getSqr());
        sojEvent.setStaticPageType(ubiEvent.getStaticPageType());
        sojEvent.setTrafficSource(ubiEvent.getTrafficSource());
        sojEvent.setUrlQueryString(ubiEvent.getUrlQueryString());
        sojEvent.setUserId(ubiEvent.getUserId());
        sojEvent.setVersion(ubiEvent.getVersion());
        sojEvent.setWebServer(ubiEvent.getWebServer());
        sojEvent.setRv(ubiEvent.isRv());
        sojEvent.setBot(RulePriorityUtils.getHighPriorityBotFlag(ubiEvent.getBotFlags()));
        if (sojEvent.getApplicationPayload() != null
            && StringUtils.isNotBlank(sojEvent.getApplicationPayload().get("ciid"))
            && !sojEvent.getApplicationPayload().get("ciid").equals("null")) {
            sojEvent.setCiid(sojEvent.getApplicationPayload().get("ciid"));
        }
        return sojEvent;
    }

    public static SojSession convertUbiSession2SojSession(UbiSession ubiSession) {
        SojSession sojSession = new SojSession();
        sojSession.setGuid(ubiSession.getGuid());
        sojSession.setSessionId(ubiSession.getSessionId());
        sojSession.setSessionSkey(ubiSession.getSessionSkey());
        sojSession.setIpv4(ubiSession.getIp());
        sojSession.setUserAgent(ubiSession.getUserAgent());
        sojSession.setSojDataDt(ubiSession.getSojDataDt());
        //change sojtimestamp to unixtimestamp
        sojSession.setSessionStartDt(SojTimestamp
                .getUnixTimestamp(ubiSession.getSessionStartDt()));
        sojSession.setStartTimestamp(ubiSession.getStartTimestamp());
        sojSession.setEndTimestamp(ubiSession.getEndTimestamp());
        // change sojtimestamp to unixtimestamp
        sojSession.setAbsStartTimestamp(
                SojTimestamp.getUnixTimestamp(ubiSession.getAbsStartTimestamp()));
        sojSession.setAbsEndTimestamp(ubiSession.getAbsEndTimestamp());
        sojSession.setBotFlagList(new ArrayList<>(ubiSession.getBotFlagList()));
        sojSession.setNonIframeRdtEventCnt(ubiSession.getNonIframeRdtEventCnt());
        sojSession.setSessionReferrer(ubiSession.getSessionReferrer());
        sojSession.setBotFlag(RulePriorityUtils.getHighPriorityBotFlag(
                ubiSession.getBotFlagList()));
        sojSession.setVersion(ubiSession.getVersion());
        sojSession.setUserId(ubiSession.getFirstUserId());
        sojSession.setSiteFlags(ubiSession.getSiteFlags());
        sojSession.setAttrFlags(ubiSession.getAttrFlags());
        sojSession.setBotFlags(ubiSession.getBotFlags());
        sojSession.setFindingFlags(ubiSession.getFindingFlags());
        sojSession.setStartPageId(ubiSession.getStartPageId());
        sojSession.setEndPageId(ubiSession.getEndPageId());
        sojSession.setDurationSec(ubiSession.getDurationSec());
        sojSession.setEventCnt(ubiSession.getEventCnt());
        sojSession.setAbsEventCnt(ubiSession.getAbsEventCnt());
        sojSession.setViCnt(ubiSession.getViCoreCnt());
        sojSession.setBidCnt(ubiSession.getBidCoreCnt());
        sojSession.setBinCnt(ubiSession.getBinCoreCnt());
        sojSession.setWatchCnt(ubiSession.getWatchCoreCnt());
        sojSession.setTrafficSrcId(ubiSession.getTrafficSrcId());
        sojSession.setAbsDuration(ubiSession.getAbsDuration());
        sojSession.setCobrand(ubiSession.getCobrand());
        sojSession.setAppId(ubiSession.getFirstAppId());
        sojSession.setSiteId(
                ubiSession.getFirstSiteId() == Integer.MIN_VALUE ? null :
                        String.valueOf(ubiSession.getFirstSiteId()));
        sojSession.setFirstSiteId(
                ubiSession.getFirstSiteId() == Integer.MIN_VALUE ? null
                        : ubiSession.getFirstSiteId());
        sojSession.setCguid(ubiSession.getFirstCguid());
        sojSession.setFirstMappedUserId(ubiSession.getFirstMappedUserId());
        sojSession.setHomepageCnt(ubiSession.getHomepageCnt());
        sojSession.setGr1Cnt(ubiSession.getGr1Cnt());
        sojSession.setGrCnt(ubiSession.getGrCnt());
        sojSession.setMyebayCnt(ubiSession.getMyebayCnt());
        sojSession.setSigninPageCnt(ubiSession.getSigninPageCnt());
        sojSession.setFirstSessionStartDt(ubiSession.getFirstSessionStartDt());
        sojSession.setSingleClickSessionFlag(ubiSession.getSingleClickSessionFlag());
        sojSession.setAsqCnt(ubiSession.getAsqCnt());
        sojSession.setAtcCnt(ubiSession.getAtcCnt());
        sojSession.setAtlCnt(ubiSession.getAtlCnt());
        sojSession.setBoCnt(ubiSession.getBoCnt());
        sojSession.setSrpCnt(ubiSession.getSrpCnt());
        sojSession.setServEventCnt(ubiSession.getServEventCnt());
        sojSession.setSearchViewPageCnt(ubiSession.getSearchViewPageCnt());
        sojSession.setBrowserFamily(ubiSession.getBrowserFamily());
        sojSession.setBrowserVersion(ubiSession.getBrowserVersion());
        sojSession.setCity(ubiSession.getCity());
        sojSession.setContinent(ubiSession.getContinent());
        sojSession.setCountry(ubiSession.getCountry());
        sojSession.setDeviceClass(ubiSession.getDeviceClass());
        sojSession.setDeviceFamily(ubiSession.getDeviceFamily());
        sojSession.setEndResourceId(ubiSession.getEndResourceId());
        sojSession.setIsReturningVisitor(ubiSession.isReturningVisitor());
        sojSession.setLineSpeed(ubiSession.getLineSpeed());
        sojSession.setOsFamily(ubiSession.getOsFamily());
        sojSession.setOsVersion(ubiSession.getOsVersion());
        sojSession.setPulsarEventCnt(ubiSession.getPulsarEventCnt());
        sojSession.setRegion(ubiSession.getRegion());
        sojSession.setSessionEndDt(ubiSession.getSessionEndDt());
        sojSession.setStartResourceId(ubiSession.getStartResourceId());
        sojSession.setStreamId(ubiSession.getStreamId());
        sojSession.setBuserId(ubiSession.getBuserId());
        sojSession.setIsOpen(ubiSession.isOpenEmit());
        //fix bugs
        sojSession.setPageId(ubiSession.getPageId());
        sojSession.setSojEventCnt(ubiSession.getAbsEventCnt());
        return sojSession;
    }

    public static long getTagMissingCnt(RawEvent rawEvent, String tagName) {
        Map<String, String> map = new HashMap<>();
        map.putAll(rawEvent.getSojA());
        map.putAll(rawEvent.getSojK());
        map.putAll(rawEvent.getSojC());

        String[] tags = tagName.split(SPLIT_DEL);
        for (String tag : tags) {
            if (map.get(tag) != null) {
                return 0;
            } else if (SOJParseClientInfo.getClientInfo(
                    rawEvent.getClientData().toString(), tag) != null) {
                return 0;
            }
        }
        return 1;
    }

    public static Double getTagValue(RawEvent rawEvent, String tagName) {
        Map<String, String> map = new HashMap<>();
        map.putAll(rawEvent.getSojA());
        map.putAll(rawEvent.getSojK());
        map.putAll(rawEvent.getSojC());

        String[] tags = tagName.split(SPLIT_DEL);
        for (String tag : tags) {
            if (StringUtils.isNotBlank(map.get(tag))) {
                try {
                    return Double.parseDouble(map.get(tag));
                } catch (Exception e) {
                    log.error("cant convert into double");
                    return 0.0;
                }
            } else if (StringUtils.isNotBlank(SOJParseClientInfo
                    .getClientInfo(rawEvent.getClientData().toString(), tag))) {
                try {
                    return Double.parseDouble(SOJParseClientInfo
                            .getClientInfo(rawEvent.getClientData().toString(), tag));
                } catch (Exception e) {
                    log.error("cant convert into double");
                    return 0.0;
                }
            }
        }
        return 0.0;
    }

    public static String getTagValueStr(RawEvent rawEvent, String tagName) {
        Map<String, String> map = new HashMap<>();
        map.putAll(rawEvent.getSojA());
        map.putAll(rawEvent.getSojK());
        map.putAll(rawEvent.getSojC());
        String[] tags = tagName.split(SPLIT_DEL);
        for (String tag : tags) {
            if (StringUtils.isNotBlank(map.get(tag))) {
                return map.get(tag);
            } else if (StringUtils.isNotBlank(SOJParseClientInfo
                    .getClientInfo(rawEvent.getClientData().toString(), tag))) {
                return SOJParseClientInfo.getClientInfo(rawEvent.getClientData().toString(), tag);
            }
        }
        return null;
    }

    public static Integer getPageId(RawEvent rawEvent) {
        try {
            Map<String, String> map = new HashMap<>();
            map.putAll(rawEvent.getSojA());
            map.putAll(rawEvent.getSojK());
            map.putAll(rawEvent.getSojC());
            String pageid = null;
            if (StringUtils.isNotBlank(map.get(Constants.P_TAG))) {
                pageid = map.get(Constants.P_TAG);
            }
            String value = IntegerField.parse(pageid);
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Parsing PageId failed, format incorrect...");
        }
        return null;
    }

    public static Integer getSiteId(RawEvent rawEvent) {
        try {
            String siteId = null;
            Map<String, String> map = new HashMap<>();
            map.putAll(rawEvent.getSojA());
            map.putAll(rawEvent.getSojK());
            map.putAll(rawEvent.getSojC());
            if (StringUtils.isNotBlank(map.get(Constants.T_TAG))) {
                siteId = map.get(Constants.T_TAG);
            }
            siteId = IntegerField.parse(siteId);
            if (StringUtils.isNotBlank(siteId)) {
                return Integer.parseInt(siteId);
            }
        } catch (Exception e) {
            log.debug("Parsing SiteId failed, format wrong...");
        }
        return null;
    }

    public static String getPageFmly(Integer pageId) {
        if (pageId != null) {
            Map<String, Map<Integer, Integer>> pageFmlyMap
                    = LkpManager.getInstance().getPageFmlyAllMaps();
            for (Map.Entry<String, Map<Integer, Integer>> entry : pageFmlyMap.entrySet()) {
                if (MapUtils.isNotEmpty(entry.getValue())
                        && entry.getValue().containsKey(pageId)) {
                    return entry.getKey();
                }
            }
        }
        return "null";
    }

    public static boolean checkIfCountIn(Integer pageId) {
        if (pageId != null) {
            Map<String, Map<Integer, Integer>> pageFmlyMap
                    = LkpManager.getInstance().getPageFmlyAllMaps();
            Set<Integer> itmPages =LkpManager.getInstance().getItmPages();
            for (Map.Entry<String, Map<Integer, Integer>> entry : pageFmlyMap.entrySet()) {
                if (itmPages.contains(pageId)&&MapUtils.isNotEmpty(entry.getValue())
                        && entry.getValue().containsKey(pageId)
                        && entry.getValue().get(pageId)==0
                ) {
                    return true;
                }
            }
        }
        return false;
    }



    public static long checkFormat(String type, String value) {
        int cnt = 0;
        if (value == null) {
            return 0;
        }
        switch (type) {
            case "Integer": {
                try {
                    Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    log.error("{} format issue,value:{}", type, value);
                    cnt = 1;
                }
                break;
            }
            case "Long": {
                try {
                    Long.parseLong(value);
                } catch (NumberFormatException e) {
                    log.error("{} format issue,value:{}", type, value);
                    cnt = 1;
                }
                break;
            }
            case "Short": {
                try {
                    Short.parseShort(value);
                } catch (NumberFormatException e) {
                    log.error("{} format issue,value:{}", type, value);
                    cnt = 1;
                }
                break;
            }
            case "Float": {
                try {
                    Float.parseFloat(value);
                } catch (NumberFormatException e) {
                    log.error("{} format issue,value:{}", type, value);
                    cnt = 1;
                }
                break;
            }
            case "Double": {
                try {
                    Double.parseDouble(value);
                } catch (NumberFormatException e) {
                    log.error("{} format issue,value:{}", type, value);
                    cnt = 1;
                }
                break;
            }
            case "Byte": {
                try {
                    Byte.parseByte(value);
                } catch (NumberFormatException e) {
                    log.error("{} format issue,value:{}", type, value);
                    cnt = 1;
                }
                break;
            }
            case "Boolean": {
                try {
                    Boolean.parseBoolean(value);
                } catch (NumberFormatException e) {
                    log.error("{} format issue,value:{}", type, value);
                    cnt = 1;
                }
                break;
            }
            case "Character":
            case "String": {
                cnt = 0;
                break;
            }
            default: {
                break;
            }
        }

        return cnt;
    }

    public static long checkFormatForU(String type, String userId) {
        try {
            if (StringUtils.isNotBlank(userId)) {
                if (IntegerField.getIntVal(userId) == null) {
                    userId = RegexReplace.replace(userId, "(\\D)+", "", 1, 0, 'i');
                    if (userId.length() > 28) {
                        return 1;
                    }
                }
                long result = Long.parseLong(userId.trim());
                if (result >= 1 && result <= 9999999999999999L) {
                    return 0;
                }
            } else {
                return 0;
            }
        } catch (Exception e) {
            log.error("Incorrect format: " + userId);
        }
        return 1;
    }

}
