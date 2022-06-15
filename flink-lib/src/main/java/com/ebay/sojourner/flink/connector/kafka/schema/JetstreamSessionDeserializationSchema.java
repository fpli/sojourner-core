package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.common.model.JetStreamOutputSession;
import com.ebay.sojourner.flink.connector.kafka.RheosEventSerdeFactory;
import io.ebay.rheos.schema.event.RheosEvent;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

@Deprecated
public class JetstreamSessionDeserializationSchema implements
    DeserializationSchema<JetStreamOutputSession> {

  @Override
  public JetStreamOutputSession deserialize(byte[] message) throws IOException {
    RheosEvent rheosEvent =
        RheosEventSerdeFactory.getRheosEventHeaderDeserializer().deserialize(null, message);
    GenericRecord genericRecord =
        RheosEventSerdeFactory.getRheosEventDeserializer().decode(rheosEvent);

    // generate jetstream output session
    Long eventCreateTimestamp = rheosEvent.getEventCreateTimestamp();
    String guid = getString(genericRecord.get("guid"));
    String sessionId = getString(genericRecord.get("sessionId"));
    String streamId = getString(genericRecord.get("streamId"));
    Long absStartTimestamp = getLong(genericRecord.get("absStartTimestamp"));
    Long absDuration = getLong(genericRecord.get("absDuration"));
    Long sessionStartDt = getLong(genericRecord.get("sessionStartDt"));
    Long sessionEndDt = getLong(genericRecord.get("sessionEndDt"));
    String ipv4 = getString(genericRecord.get("ipv4"));
    String userAgent = getString(genericRecord.get("userAgent"));
    String referer = getString(genericRecord.get("referer"));
    Integer botFlag = getInteger(genericRecord.get("botFlag"));
    Integer startResourceId = getInteger(genericRecord.get("startResourceId"));
    Integer endResourceId = getInteger(genericRecord.get("endResourceId"));
    Integer pageId = getInteger(genericRecord.get("pageId"));
    Integer viCnt = getInteger(genericRecord.get("viCnt"));
    Integer bidCnt = getInteger(genericRecord.get("bidCnt"));
    Integer binCnt = getInteger(genericRecord.get("binCnt"));
    Integer watchCnt = getInteger(genericRecord.get("watchCnt"));
    Integer homepageCnt = getInteger(genericRecord.get("homepageCnt"));
    Integer boCnt = getInteger(genericRecord.get("boCnt"));
    Integer srpCnt = getInteger(genericRecord.get("srpCnt"));
    Integer asqCnt = getInteger(genericRecord.get("asqCnt"));
    Integer atcCnt = getInteger(genericRecord.get("atcCnt"));
    Integer atlCnt = getInteger(genericRecord.get("atlCnt"));
    Integer absEventCnt = getInteger(genericRecord.get("absEventCnt"));
    Integer eventCnt = getInteger(genericRecord.get("eventCnt"));
    Integer pulsarEventCnt = getInteger(genericRecord.get("pulsarEventCnt"));
    Integer sojEventCnt = getInteger(genericRecord.get("sojEventCnt"));
    Integer servEventCnt = getInteger(genericRecord.get("servEventCnt"));
    Integer viewEventCnt = getInteger(genericRecord.get("viewEventCnt"));
    Integer searchViewPageCnt = getInteger(genericRecord.get("searchViewPageCnt"));
    Integer trafficSrcId = getInteger(genericRecord.get("trafficSrcId"));
    String lineSpeed = getString(genericRecord.get("lineSpeed"));
    Boolean isReturningVisitor = getBoolean(genericRecord.get("isReturningVisitor"));
    Integer appId = getInteger(genericRecord.get("appId"));
    String browserVersion = getString(genericRecord.get("browserVersion"));
    String browserFamily = getString(genericRecord.get("browserFamily"));
    String osVersion = getString(genericRecord.get("osVersion"));
    String osFamily = getString(genericRecord.get("osFamily"));
    String deviceClass = getString(genericRecord.get("deviceClass"));
    String deviceFamily = getString(genericRecord.get("deviceFamily"));
    String city = getString(genericRecord.get("city"));
    String region = getString(genericRecord.get("region"));
    String country = getString(genericRecord.get("country"));
    String continent = getString(genericRecord.get("continent"));
    Boolean singleClickSessionFlag = getBoolean(genericRecord.get("singleClickSessionFlag"));
    Integer cobrand = getInteger(genericRecord.get("cobrand"));
    String siteId = getString(genericRecord.get("siteId"));
    String cguid = getString(genericRecord.get("cguid"));
    String userId = getString(genericRecord.get("userId"));
    String buserId = getString(genericRecord.get("buserId"));

    return new JetStreamOutputSession(guid, eventCreateTimestamp, sessionId, streamId,
        absStartTimestamp, absDuration, sessionStartDt, sessionEndDt, ipv4, userAgent, referer,
        botFlag, startResourceId, endResourceId, pageId, viCnt, bidCnt, binCnt, watchCnt,
        homepageCnt, boCnt, srpCnt, asqCnt, atcCnt, atlCnt, absEventCnt, eventCnt, pulsarEventCnt,
        sojEventCnt, servEventCnt, viewEventCnt, searchViewPageCnt, trafficSrcId, lineSpeed,
        isReturningVisitor, appId, browserVersion, browserFamily, osVersion, osFamily, deviceClass,
        deviceFamily, city, region, country, continent, singleClickSessionFlag, cobrand, siteId,
        cguid, userId, buserId);
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
  public boolean isEndOfStream(JetStreamOutputSession nextElement) {
    return false;
  }

  @Override
  public TypeInformation<JetStreamOutputSession> getProducedType() {
    return TypeInformation.of(JetStreamOutputSession.class);
  }
}
