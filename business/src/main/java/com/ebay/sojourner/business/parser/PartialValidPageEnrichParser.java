package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.ClientData;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class PartialValidPageEnrichParser implements FieldParser<RawEvent, UbiEvent> {
  private Integer[] expmPageIds = {2527563,2536688,2530661,3134835};

  @Override
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {
    Map<String, String> applicationPayload = new HashMap<>();
    if(rawEvent.getSojA() != null) {
      applicationPayload.putAll(rawEvent.getSojA());
    }
    if(rawEvent.getSojK() != null) {
      applicationPayload.putAll(rawEvent.getSojK());
    }
    if(rawEvent.getSojC() != null) {
      applicationPayload.putAll(rawEvent.getSojC());
    }
    Integer pageId = ubiEvent.getPageId();
    String urlQueryString = ubiEvent.getUrlQueryString();
    ClientData clientData = ubiEvent.getClientData();
    String webServer = ubiEvent.getWebServer();
    String sqr = ubiEvent.getSqr();
    Integer siteId = ubiEvent.getSiteId();
    String app =
            ubiEvent.getApplicationPayload() == null
                    ? null
                    : applicationPayload.get("app");
    String agentString = clientData == null ? null : clientData.getAgent();
    boolean isSpecialApp = "2571".equals(app) || "1462".equals(app) || "2878".equals(app);

    // enrich on 2024-08-20, background in https://jirap.corp.ebay.com/browse/BHVRDT-13391
    if(!ubiEvent.isPartialValidPage()){
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 2351460
            && app != null
            && (app.equals("1462") || app.equals("2878"))
            && applicationPayload.get("sHit") == null) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 2385738
            && (app == null || app.equals("3564"))
            && agentString.toLowerCase().startsWith("ebay")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 2487283
            && StringUtils.isNotBlank(webServer)
            && StringUtils.isNotBlank(urlQueryString)
            && webServer.endsWith(".ebay.com")
            && urlQueryString.toLowerCase().startsWith("/ws/ebayisapi.dll?signinauthredirect=&guid=true")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 2050445
            && StringUtils.isNotBlank(webServer)
            && webServer.startsWith("rover.ebay.")
            && "new".equalsIgnoreCase(applicationPayload.get("cguidsrc"))) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (!isCorrespondingPageId(pageId, expmPageIds)
            && "expm".equalsIgnoreCase(applicationPayload.get("eactn"))) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if (pageId == 2380424
            && app != null
            && (app.equals("1462") || app.equals("2878"))) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if (pageId == 2380424
            && app != null
            && app.equals("2571")
            && siteId==0) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if ((pageId == 4375194 || pageId == 2481888)
            && StringUtils.isNotBlank(urlQueryString)
            && urlQueryString.toLowerCase().startsWith("/?_trkparms=")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if (StringUtils.isNotBlank(webServer)
            && webServer.contains(".ebaystores.")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if ((pageId == 2047675 || pageId == 2349624)
            && StringUtils.isNotBlank(urlQueryString)
            && urlQueryString.toLowerCase().contains("autorefresh")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if (pageId == 2323438) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if ((pageId == 2045573 || pageId == 2053742)
            && StringUtils.isNotBlank(urlQueryString)
            && urlQueryString.toLowerCase().startsWith("/sch/ajax/predict")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if (StringUtils.isNotBlank(webServer)
            && webServer.contains("latest.")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if (StringUtils.isNotBlank(urlQueryString)
            && urlQueryString.toLowerCase().startsWith("/sch/ajax/predict")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    if (StringUtils.isNotBlank(urlQueryString)) {
      String pattern = ".*?mpre.*?google.*?asnc.*?";
      boolean isMatch = Pattern.matches(pattern, urlQueryString);
      if(isMatch){
        setPartialValidFlag(false,  ubiEvent);
        return;
      }
    }
    if (pageId == 2322147
            && StringUtils.isNotBlank(urlQueryString)
            && urlQueryString.toLowerCase().startsWith("/findproduct/tracking")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (StringUtils.isNotBlank(agentString)
            && agentString.toLowerCase().startsWith("swcd")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (applicationPayload.containsKey("rdthttps")
            && applicationPayload.get("sHit") == null) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (StringUtils.isNotBlank(agentString)) {
      String pattern = "Dalvik.*?Android.*?";
      boolean isMatch = Pattern.matches(pattern, agentString);
      if(isMatch){
        setPartialValidFlag(false,  ubiEvent);
        return;
      }
    }
    if (pageId == 2376473
            && app != null
            && (app.equals("1462") || app.equals("2878"))
            && applicationPayload.containsKey("mav")
            && NumberUtils.isCreatable(applicationPayload.get("mav"))
            && Float.parseFloat(applicationPayload.get("mav"))>=5.36
            && Float.parseFloat(applicationPayload.get("mav"))<6) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 1881
            && 1 == ubiEvent.getSeqNum()) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if ((pageId == 2058891 || pageId == 2057641)
            && siteId != null
            && siteId == 0
            && StringUtils.isNotBlank(webServer)) {
      String pattern = ".*?stratus.*?ebay.*?";
      boolean isMatch = Pattern.matches(pattern, agentString);
      if(isMatch){
        setPartialValidFlag(false,  ubiEvent);
        return;
      }
    }
    if ((pageId == 2045573 || pageId == 2053742)
            && ("update".equalsIgnoreCase(sqr)
                || "/sch/update".equalsIgnoreCase(urlQueryString))) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 2065432
            && "2571".equals(app)) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 2543464
            && StringUtils.isNotBlank(agentString)
            && agentString.toLowerCase().contains("darwin")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 3276719
            && StringUtils.isNotBlank(urlQueryString)
            && urlQueryString.toLowerCase().startsWith("/sl/prelist/api/suggest")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if ("LST".equals(applicationPayload.get("efam"))
            && "SRCH".equals(applicationPayload.get("eactn"))) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if ((pageId == 3186120 || pageId == 3186125)
            && StringUtils.isNotBlank(urlQueryString)
            && urlQueryString.toLowerCase().contains("ajax")) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 3289402
            && "true".equals(applicationPayload.get("poll"))){
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == -999
            && isSpecialApp) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }
    if (pageId == 4451299
            && !isSpecialApp) {
      setPartialValidFlag(false,  ubiEvent);
      return;
    }

    setPartialValidFlag(true,  ubiEvent);
  }

  private boolean isCorrespondingPageId(Integer id, Integer[] pageIdList) {
    for (Integer pageId : pageIdList) {
      if (pageId.equals(id)) {
        return true;
      }
    }
    return false;
  }

  private boolean isCorrespondingString(String source, String[] matchStr) {
    for (String str : matchStr) {
      if (str.equals(source)) {
        return true;
      }
    }
    return false;
  }
  private void setPartialValidFlag(Boolean flag, UbiEvent ubiEvent){
    if(flag == null || !flag){
      ubiEvent.setPartialValidPageFlag(false);
    }else{
      ubiEvent.setPartialValidPageFlag(true);
      ubiEvent.setApplicationPayload(ubiEvent.getApplicationPayload() + "&spvpf=1");
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
