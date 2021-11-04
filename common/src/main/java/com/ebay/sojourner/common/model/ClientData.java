package com.ebay.sojourner.common.model;

import com.ebay.sojourner.common.util.CalTimeOfDay;
import com.ebay.sojourner.common.util.PropertyUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Data
@Slf4j
public class ClientData {

  private String forwardFor;
  private String script;
  private String server;
  private String tMachine;
  private String tStamp;
  private String tName;
  private String t;
  private String colo;
  private String pool;
  private String agent;
  private String remoteIP;
  private String tType;
  private String tPool;
  private String tStatus;
  private String corrId = "";
  private String contentLength;
  private String nodeId = "";
  private String requestGuid = "";
  private String urlQueryString;
  private String referrer;
  private String rlogid = "";
  private String acceptEncoding;
  private String tDuration;
  private String encoding;
  private String tPayload;
  private String chUaModel;
  private String chUaPlatformVersion;
  private String chUaFullVersion;
  private String chUaMobile;

  @Override
  public String toString() {
    StringBuilder clientInfo = new StringBuilder();

    clientInfo.append("TPayload=").append(tPayload);

    if (StringUtils.isNotEmpty(tPool)) {
      String tagValue = PropertyUtils.encodeValue(tPool);
      clientInfo.append("&TPool=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(tDuration)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(tDuration);
      clientInfo.append("TDuration=").append(tagValue);

    }
    if (StringUtils.isNotEmpty(tStatus)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(tStatus);
      clientInfo.append("TStatus=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(tType)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(tType);
      clientInfo.append("TType=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(contentLength)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(contentLength);
      clientInfo.append("ContentLength=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(forwardFor)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(forwardFor);
      clientInfo.append("ForwardedFor=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(script)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(script);
      clientInfo.append("Script=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(server)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(server);
      clientInfo.append("Server=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(tMachine)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(tMachine);
      clientInfo.append("TMachine=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(tStamp)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(tStamp);
      String tstamp = toCALDateString(Long.valueOf(tagValue));
      clientInfo.append("TStamp=").append(tstamp);
    }
    if (StringUtils.isNotEmpty(tName)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(tName);
      clientInfo.append("TName=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(agent)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(agent);
      clientInfo.append("Agent=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(remoteIP)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(remoteIP);
      clientInfo.append("RemoteIP=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(encoding)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(encoding);
      clientInfo.append("Encoding=").append(tagValue);
    }
    // Referer must be in the end of clientData since it has nested '&' '='
    // if (referrer != null && !referrer.equals("") && !referrer.equalsIgnoreCase("null")) {
    if (StringUtils.isNotEmpty(referrer)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(referrer);
      clientInfo.append("Referer=").append(tagValue);
    }

    if (StringUtils.isNotEmpty(corrId)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(corrId);
      clientInfo.append("corrId=").append(tagValue);
    }

    if (StringUtils.isNotEmpty(nodeId)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(nodeId);
      clientInfo.append("nodeId=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(chUaModel)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(chUaModel);
      clientInfo.append("chUaModel=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(chUaPlatformVersion)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(chUaPlatformVersion);
      clientInfo.append("chUaPlatformVersion=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(chUaFullVersion)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(chUaFullVersion);
      clientInfo.append("chUaFullVersion=").append(tagValue);
    }
    if (StringUtils.isNotEmpty(chUaMobile)) {
      if (clientInfo.length() > 0) {
        clientInfo.append("&");
      }
      String tagValue = PropertyUtils.encodeValue(chUaMobile);
      clientInfo.append("chUaMobile=").append(tagValue);
    }

    return clientInfo.toString();
  }

  private String toCALDateString(long time) {
    CalTimeOfDay calTimeOfDay = new CalTimeOfDay(time);
    return calTimeOfDay.toString();
  }


}
