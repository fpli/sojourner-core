package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.common.model.JetStreamOutputEvent;
import com.ebay.sojourner.flink.connector.kafka.RheosEventSerdeFactory;
import io.ebay.rheos.schema.event.RheosEvent;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@Deprecated
public class JetstreamEventDeserializationSchema implements
    DeserializationSchema<JetStreamOutputEvent> {

  @Override
  public JetStreamOutputEvent deserialize(byte[] message) throws IOException {
    RheosEvent rheosEvent =
        RheosEventSerdeFactory.getRheosEventHeaderDeserializer().deserialize(null, message);
    GenericRecord genericRecord =
        RheosEventSerdeFactory.getRheosEventDeserializer().decode(rheosEvent);

    // generate jetstream output event
    long eventCreateTimestamp = rheosEvent.getEventCreateTimestamp();
    String guid = getString(genericRecord.get("guid"));
    Long eventTimestamp = getLong(genericRecord.get("eventTimestamp"));
    String sid = getString(genericRecord.get("sid"));
    Long eventCaptureTime = getLong(genericRecord.get("eventCaptureTime"));
    String requestCorrelationId = getString(genericRecord.get("requestCorrelationId"));
    String pageFamily = getString(genericRecord.get("pageFamily"));
    String remoteIP = getString(genericRecord.get("remoteIP"));
    String appVersion = getString(genericRecord.get("appVersion"));
    String eventFamily = getString(genericRecord.get("eventFamily"));
    String eventAction = getString(genericRecord.get("eventAction"));
    String trafficSource = getString(genericRecord.get("trafficSource"));
    String osVersion = getString(genericRecord.get("osVersion"));
    String deviceFamily = getString(genericRecord.get("deviceFamily"));
    String deviceType = getString(genericRecord.get("deviceType"));
    String browserVersion = getString(genericRecord.get("browserVersion"));
    String browserFamily = getString(genericRecord.get("browserFamily"));
    String osFamily = getString(genericRecord.get("osFamily"));
    String enrichedOsVersion = getString(genericRecord.get("enrichedOsVersion"));
    String rlogid = getString(genericRecord.get("rlogid"));
    String sessionId = getString(genericRecord.get("sessionId"));
    String cguid = getString(genericRecord.get("cguid"));
    Integer pageId = getInteger(genericRecord.get("pageId"));
    String pageName = getString(genericRecord.get("pageName"));
    String userId = getString(genericRecord.get("userId"));
    String clickId = getString(genericRecord.get("clickId"));
    String siteId = getString(genericRecord.get("siteId"));
    String seqNum = getString(genericRecord.get("seqNum"));
    String ciid = getString(genericRecord.get("ciid"));
    String siid = getString(genericRecord.get("siid"));
    Integer rdt = getInteger(genericRecord.get("rdt"));
    Integer regu = getInteger(genericRecord.get("regu"));
    boolean iframe = getBoolean(genericRecord.get("iframe"));
    String refererHash = getString(genericRecord.get("refererHash"));
    String sqr = getString(genericRecord.get("sqr"));
    String itemId = getString(genericRecord.get("itemId"));
    String flags = getString(genericRecord.get("flags"));
    String urlQueryString = getString(genericRecord.get("urlQueryString"));
    String webServer = getString(genericRecord.get("webServer"));
    String cookies = getString(genericRecord.get("cookies"));
    Integer bot = getInteger(genericRecord.get("bot"));
    String clientIP = getString(genericRecord.get("clientIP"));
    String agentInfo = getString(genericRecord.get("agentInfo"));
    String appId = getString(genericRecord.get("appId"));
    String cobrand = getString(genericRecord.get("cobrand"));
    Map<Utf8, Utf8> applicationPayload = (Map<Utf8, Utf8>) genericRecord.get("applicationPayload");
    Map<Utf8, Utf8> clientData = (Map<Utf8, Utf8>) genericRecord.get("clientData");

    Map<String, String> applicationPayloadMap = new HashMap<>();
    if (applicationPayload != null) {
      for (Map.Entry<Utf8, Utf8> entry : applicationPayload.entrySet()) {
        applicationPayloadMap.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    Map<String, String> clientDataMap = new HashMap<>();
    if (clientData != null) {
      for (Map.Entry<Utf8, Utf8> entry : clientData.entrySet()) {
        clientDataMap.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    return new JetStreamOutputEvent(guid, eventTimestamp, eventCreateTimestamp, sid,
        eventCaptureTime, requestCorrelationId, pageFamily, remoteIP, appVersion, eventFamily,
        eventAction, trafficSource, osVersion, deviceFamily, deviceType, browserVersion,
        browserFamily, osFamily, enrichedOsVersion, rlogid, sessionId, cguid, pageId, pageName,
        userId, clickId, siteId, seqNum, ciid, siid, rdt, regu, iframe, refererHash, sqr, itemId,
        flags, urlQueryString, webServer, cookies, bot, clientIP, agentInfo, appId, cobrand,
        applicationPayloadMap, clientDataMap);
  }

  private Integer getInteger(Object o) {
    if (StringUtils.isEmpty(getString(o))) {
      return null;
    } else {
      return Integer.valueOf(getString(o));
    }
  }

  private boolean getBoolean(Object o) {
    if (StringUtils.isEmpty(getString(o))) {
      return false;
    } else {
      return Boolean.valueOf(getString(o));
    }
  }


  private Long getLong(Object o) {
    if (StringUtils.isEmpty(getString(o))) {
      return null;
    } else {
      return Long.valueOf(getString(o));
    }
  }

  private String getString(Object o) {
    return (o != null && !"null".equals(o.toString())) ? o.toString() : null;
  }

  @Override
  public boolean isEndOfStream(JetStreamOutputEvent nextElement) {
    return false;
  }

  @Override
  public TypeInformation<JetStreamOutputEvent> getProducedType() {
    return TypeInformation.of(JetStreamOutputEvent.class);
  }
}
