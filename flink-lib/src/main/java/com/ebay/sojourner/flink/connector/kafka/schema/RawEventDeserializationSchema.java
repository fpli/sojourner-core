package com.ebay.sojourner.flink.connector.kafka.schema;

import com.ebay.sojourner.common.model.ClientData;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.RheosHeader;
import com.ebay.sojourner.common.util.CalTimeOfDay;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.SojTimestampParser;
import com.ebay.sojourner.flink.connector.kafka.RheosEventSerdeFactory;
import io.ebay.rheos.schema.event.RheosEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RawEventDeserializationSchema implements DeserializationSchema<RawEvent> {

  private static final DateTimeFormatter formaterUtc =
      DateTimeFormat.forPattern(Constants.DEFAULT_TIMESTAMP_FORMAT)
          .withZone(
              DateTimeZone.forTimeZone(Constants.UTC_TIMEZONE));
  private static final DateTimeFormatter formater = DateTimeFormat.forPattern(
      Constants.DEFAULT_TIMESTAMP_FORMAT)
      .withZone(
          DateTimeZone.forTimeZone(Constants.PST_TIMEZONE));
  private static final DateTimeFormatter dateMinsFormatter = DateTimeFormat.forPattern(
      Constants.DEFAULT_DATE_MINS_FORMAT)
      .withZone(
          DateTimeZone.forTimeZone(Constants.PST_TIMEZONE));
  private static String[] tagsToEncode = new String[]{Constants.TAG_ITEMIDS, Constants.TAG_TRKP};

  private String schemaRegistryUrl = null;

  private static final String G_TAG = "g";

  private transient Counter droppedEventCounter;

  public RawEventDeserializationSchema() { }

  public RawEventDeserializationSchema(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    this.droppedEventCounter = context.getMetricGroup()
                                      .addGroup(Constants.SOJ_METRIC_TYPE)
                                      .counter(Constants.SOJ_METRIC_DROPPED_EVENT_CNT);
  }

  @Override
  public RawEvent deserialize(byte[] message) throws IOException {
    long ingestTime = new Date().getTime();
    long messageSize = message.length;
    RheosEvent rheosEvent =
        RheosEventSerdeFactory.getRheosEventHeaderDeserializer().deserialize(null, message);
    GenericRecord genericRecord = null;
    try {
      genericRecord = RheosEventSerdeFactory.getRheosEventDeserializer(schemaRegistryUrl)
                                            .decode(rheosEvent);
    } catch (Exception e) {
      log.error("Error when deserializing RawEvent from Pathfinder, schemaId: {}, timestamp: {}",
                rheosEvent.getSchemaId(), rheosEvent.getEventCreateTimestamp(), e);
      droppedEventCounter.inc();
      return null;
    }

    // Generate RheosHeader
    RheosHeader rheosHeader =
        new RheosHeader(
            rheosEvent.getEventCreateTimestamp(),
            rheosEvent.getEventSentTimestamp(),
            rheosEvent.getSchemaId(),
            rheosEvent.getEventId(),
            rheosEvent.getProducerId());

    // Generate sojA, sojK, sojC
    Map<Utf8, Utf8> sojA = (Map<Utf8, Utf8>) genericRecord.get("sojA");
    Map<Utf8, Utf8> sojK = (Map<Utf8, Utf8>) genericRecord.get("sojK");
    Map<Utf8, Utf8> sojC = (Map<Utf8, Utf8>) genericRecord.get("sojC");
    Map<String, String> sojAMap = new HashMap<>();
    if (sojA != null) {
      for (Map.Entry<Utf8, Utf8> entry : sojA.entrySet()) {
        sojAMap.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }
    Map<String, String> sojKMap = new HashMap<>();
    if (sojK != null) {
      for (Map.Entry<Utf8, Utf8> entry : sojK.entrySet()) {
        sojKMap.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    Map<String, String> sojCMap = new HashMap<>();
    if (sojC != null) {
      for (Map.Entry<Utf8, Utf8> entry : sojC.entrySet()) {
        sojCMap.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }
    encodeTags(sojAMap);
    encodeTags(sojKMap);
    encodeTags(sojCMap);

    // ignore no 'g' tag events
    Map<String, String> allMap = new HashMap<>();
    allMap.putAll(sojAMap);
    allMap.putAll(sojCMap);
    allMap.putAll(sojKMap);
    if (!allMap.containsKey(G_TAG)) {
      log.info("The Event from pathfinder is invalid: {}", allMap);
      return null;
    }

    // Generate ClientData
    // If clientData is not of type GenericRecord, just skip this message.
    if (!(genericRecord.get("clientData") instanceof GenericRecord)) {
      log.info("clientData is not of type GenericRecord. " + genericRecord.get("clientData"));
      return null;
    }

    GenericRecord genericClientData = (GenericRecord) genericRecord.get("clientData");
    ClientData clientData = new ClientData();
    parseClientData(clientData, genericClientData);
    RawEvent rawEvent = new RawEvent(rheosHeader, sojAMap, sojKMap, sojCMap, clientData,
        ingestTime, null, messageSize, null);
    SojTimestampParser.parseEventTimestamp(rawEvent);
    return rawEvent;
  }

  private void parseClientData(ClientData clentData, GenericRecord genericRecord) {
    Object forwardFor = genericRecord.get("ForwardFor");
    clentData.setForwardFor(forwardFor != null ? forwardFor.toString() : "");

    Object script = genericRecord.get("Script");
    clentData.setScript(script != null ? script.toString() : "");

    Object server = genericRecord.get("Server");
    clentData.setServer(server != null ? server.toString() : "");

    Object machine = genericRecord.get("TMachine");
    clentData.setTMachine(machine != null ? machine.toString() : "");

    Object name = genericRecord.get("TName");
    clentData.setTName(name != null ? name.toString() : "");

    Object stamp = genericRecord.get("TStamp");
    if (stamp != null) {
      clentData.setTStamp(stamp.toString());
    }

    Object agent = genericRecord.get("agent");
    clentData.setAgent(agent != null ? agent.toString() : "");

    Object pool = genericRecord.get("pool");
    clentData.setTPool(pool != null ? pool.toString() : "");

    Object remoteIp = genericRecord.get("remoteIP");
    clentData.setRemoteIP(remoteIp != null ? remoteIp.toString() : "");

    Object type = genericRecord.get("TType");
    clentData.setTType(type != null ? type.toString() : "");

    Object status = genericRecord.get("TStatus");
    clentData.setTStatus(status != null ? status.toString() : "");

    Object contentLength = genericRecord.get("contentLength");
    clentData.setContentLength(contentLength != null ? contentLength.toString() : "");

    Object urlQueryString = genericRecord.get("urlQueryString");
    clentData.setUrlQueryString(urlQueryString != null ? urlQueryString.toString() : "");

    Object referrer = genericRecord.get("referrer");  //
    clentData.setReferrer(referrer != null ? referrer.toString() : "");

    Object corrId = genericRecord.get("corrId");
    clentData.setCorrId(corrId != null ? corrId.toString() : "");

    Object nodeId = genericRecord.get("nodeId");
    clentData.setNodeId(nodeId != null ? nodeId.toString() : "");

    Object requestGuid = genericRecord.get("requestGuid");
    clentData.setRequestGuid(requestGuid != null ? requestGuid.toString() : "");

    Object rlogid = genericRecord.get("rlogid");
    clentData.setRlogid(rlogid != null ? rlogid.toString() : "");

    Object colo = genericRecord.get("colo");
    clentData.setColo(colo != null ? colo.toString() : "");

    Object acceptEncoding = genericRecord.get("acceptEncoding");
    clentData.setEncoding(acceptEncoding != null ? acceptEncoding.toString() : "");

    Object tDuration = genericRecord.get("TDuration");
    clentData.setTDuration(tDuration != null ? tDuration.toString() : "");

    // add tags for UA-CH project
    // (ch_ua_model,ch_ua_platform_version,ch_ua_full_version,ch_ua_mobile)
    Object chUaModel = genericRecord.get("ch_ua_model");
    clentData.setChUaModel(chUaModel != null ? chUaModel.toString() : "");

    Object chUaPlatformVersion = genericRecord.get("ch_ua_platform_version");
    clentData.setChUaPlatformVersion(
            chUaPlatformVersion != null ? chUaPlatformVersion.toString() : "");

    Object chUaFullVersion = genericRecord.get("ch_ua_full_version");
    clentData.setChUaFullVersion(chUaFullVersion != null ? chUaFullVersion.toString() : "");

    Object chUaMobile = genericRecord.get("ch_ua_mobile");
    clentData.setChUaMobile(chUaMobile != null ? chUaMobile.toString() : "");

    // TODO will deprecated when all applied new schema
    if (genericRecord.get("TPayload") != null) {
      parseTPayload(clentData, genericRecord.get("TPayload").toString());
    } else {
      String tPaylload = constuctTPayload(clentData, null);
      clentData.setTPayload(tPaylload);
    }
  }

  private void parseTPayload(ClientData clientData, String tpayload) {

    try {
      String decoded;
      int index = tpayload.indexOf("=");
      if (index > 0) {
        decoded = "&" + tpayload;
      } else {
        decoded = URLDecoder.decode("&" + tpayload, "UTF-8");
      }
      if (decoded == null) {
        return;
      }
      String app = getTag(tpayload, "application_name");

      if (clientData.getRlogid() == null || clientData.getRlogid().equals("")) {
        String rlogid = getTag(decoded, "rlogid");
        clientData.setRlogid(rlogid != null ? rlogid : "");
      }

      if (clientData.getRlogid() == null || clientData.getRlogid().equals("")) {
        String rlogid = getTag(decoded, "logid");
        clientData.setRlogid(rlogid != null ? rlogid : "");
      }

      if (clientData.getUrlQueryString() == null || clientData.getUrlQueryString()
          .equals("")) {
        String urlQueryString = getTag(decoded, "urlQueryString");
        clientData.setUrlQueryString(urlQueryString != null ? urlQueryString : "");
      }

      if (clientData.getUrlQueryString() == null || clientData.getUrlQueryString()
          .equals("")) {
        String url = getTag(decoded, "url");
        clientData.setUrlQueryString(url != null ? url : "");
      }

      if (clientData.getAgent() == null || clientData.getAgent().equals("")) {
        String agent = getTag(decoded, "Agent");
        clientData.setAgent(agent != null ? agent : "");
      }

      if (clientData.getReferrer() == null || clientData.getReferrer().equals("")) {
        String referer = getTag(decoded, "Referer");
        clientData.setReferrer(referer != null ? referer : "");
      }

      if (clientData.getRemoteIP() == null || clientData.getRemoteIP().equals("")) {
        String remoteIP = getTag(decoded, "RemoteIP");
        clientData.setRemoteIP(remoteIP != null ? remoteIP : "");
      }

      if (clientData.getForwardFor() == null || clientData.getForwardFor().equals("")) {
        String forwardFor = getTag(decoded, "ForwardFor");
        clientData.setForwardFor(forwardFor != null ? forwardFor : "");
      }

      if (clientData.getTPool() == null || clientData.getTPool().equals("")) {
        String tPool = getTag(decoded, "TPool");
        clientData.setTPool(tPool != null ? tPool : "");
      }

      if (clientData.getTType() == null || clientData.getTType().equals("")) {
        String tType = getTag(decoded, "TType");
        clientData.setTType(tType != null ? tType : "");
      }

      if (clientData.getTStatus() == null || clientData.getTStatus().equals("")) {
        String tStatus = getTag(decoded, "TStatus");
        clientData.setTStatus(tStatus != null ? tStatus : "");

      }

      if (clientData.getTType() == null || clientData.getTType().equals("")) {
        String tType = getTag(decoded, "TType");
        clientData.setTType(tType != null ? tType : "");
      }

      if (clientData.getEncoding() == null || clientData.getEncoding().equals("")) {
        String encoding = getTag(decoded, "Encoding");
        clientData.setEncoding(encoding != null ? encoding : "");
      }

      if (clientData.getTDuration() == null || clientData.getTDuration().equals("")) {
        String tDuration = getTag(decoded, "TDuration");
        clientData.setTDuration(tDuration != null ? tDuration : "");
      }

      // TODO will deprecate after TPool set for colweb
      if ((clientData.getTPool() == null || clientData.getTPool().equals("")) && app != null
          && app.equals("colweb")) {
        clientData.setTPool("r1colweb");
      }
      String tPaylload = constuctTPayload(clientData, decoded);
      clientData.setTPayload(tPaylload);

    } catch (Exception e) {
      log.error("Error when parsing TPayload.", tpayload, e);
    }
  }

  private String getTag(String str, String tagName) {
    int index = str.indexOf("&" + tagName + "=");
    if (index >= 0) {
      int nextIndex = str.indexOf('&', index + 1);
      String tagVal = null;
      if (nextIndex > 0) {
        tagVal = str.substring(index + tagName.length() + 2, nextIndex);
      } else {
        tagVal = str.substring(index + tagName.length() + 2);
      }

      if (tagVal != null) {
        try {
          return URLDecoder.decode(tagVal, "UTF-8");
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }

  private void encodeTags(Map<String, String> tags) {
    for (String keyToEncode : tagsToEncode) {
      if (tags.containsKey(keyToEncode)) {
        String value = tags.get(keyToEncode);
        if (value != null && value.contains("=")) {
          try {
            tags.put(keyToEncode, URLEncoder.encode(value, "UTF-8"));

          } catch (UnsupportedEncodingException e) {
            log.trace("Fail to encode tags. ", e);
          }
        }
      }
    }
  }

  private String constuctTPayload(ClientData clientData, String decodedTPayload) {
    try {
      StringBuilder tpStr = new StringBuilder();
      tpStr.append("corr_id_").append("=").append(clientData.getCorrId());
      tpStr.append("&").append("node_id").append("=").append(clientData.getNodeId());
      tpStr.append("&").append("REQUEST_GUID").append("=").append(clientData.getRequestGuid());
      tpStr.append("&").append("logid").append("=").append(clientData.getRlogid());
      if (decodedTPayload != null) {
        String calMod = getTag(decodedTPayload, "cal_mod");
        if (calMod != null && !calMod.equals("")) {
          tpStr.append("&").append("cal_mod").append("=").append(calMod);
        }
        String country = getTag(decodedTPayload, "country");
        if (country != null && !country.equals("")) {
          tpStr.append("&").append("country").append("=").append(country);
        }
        String isResponseGzipped = getTag(decodedTPayload, "isResponseGzipped");
        if (isResponseGzipped != null && !isResponseGzipped.equals("")) {
          tpStr.append("&").append("isResponseGzipped").append("=").append(isResponseGzipped);
        }
        String jct = getTag(decodedTPayload, "jct");
        if (jct != null && !jct.equals("")) {
          tpStr.append("&").append("jct").append("=").append(jct);
        }
        String lang = getTag(decodedTPayload, "lang");
        if (lang != null && !lang.equals("")) {
          tpStr.append("&").append("lang").append("=").append(lang);
        }
        String ri = getTag(decodedTPayload, "ri");
        if (ri != null && !ri.equals("")) {
          tpStr.append("&").append("ri").append("=").append(ri);
        }
        String tt = getTag(decodedTPayload, "tt");
        if (tt != null && !tt.equals("")) {
          tpStr.append("&").append("tt").append("=").append(tt);
        }
        String statusCode = getTag(decodedTPayload, "statusCode");
        if (statusCode != null && !statusCode.equals("")) {
          tpStr.append("&").append("statusCode").append("=").append(statusCode);
        }
      }
      String transactionTPayload = URLEncoder.encode(tpStr.toString(), "UTF-8");
      return encodePayloadForSpecialCharacters(transactionTPayload);

    } catch (UnsupportedEncodingException e) {
      log.error("encoding failed:" + e);
    }
    return null;

  }

  public String encodePayloadForSpecialCharacters(String payLoad) {
    if (payLoad == null) {
      return null;
    }
    StringBuilder newEncodedString = new StringBuilder();
    for (int nLen = 0; nLen < payLoad.length(); nLen++) {
      if (payLoad.charAt(nLen) == '*') {
        newEncodedString.append("%2A");
      } else if (payLoad.charAt(nLen) == '+') {
        newEncodedString.append("%20");
      } else {
        newEncodedString.append(payLoad.charAt(nLen));
      }
    }
    return newEncodedString.toString();
  }

  public String toCALDateString(long time) {
    CalTimeOfDay calTimeOfDay = new CalTimeOfDay(time);
    return calTimeOfDay.toString();

  }
  // move the logic to SojTimestampParser utils
  //  private void parseEventTimestamp(RawEvent rawEvent) {
  //    StringBuilder buffer = new StringBuilder();
  //    Long abEventTimestamp;
  //    Long eventTimestamp;
  //    Long interval;
  //    String mtstsString;
  //    String pageId = null;
  //    Map<String, String> map = new HashMap<>();
  //    map.putAll(rawEvent.getSojA());
  //    map.putAll(rawEvent.getSojK());
  //    map.putAll(rawEvent.getSojC());
  //    String applicationPayload = null;
  //    String mARecString = PropertyUtils.mapToString(rawEvent.getSojA());
  //    String mKRecString = PropertyUtils.mapToString(rawEvent.getSojK());
  //    String mCRecString = PropertyUtils.mapToString(rawEvent.getSojC());
  //    if (mARecString != null) {
  //      applicationPayload = mARecString;
  //    }
  //    if ((applicationPayload != null) && (mKRecString != null)) {
  //      applicationPayload = applicationPayload + "&" + mKRecString;
  //    }
  //
  //    // else set C record
  //    if (applicationPayload == null) {
  //      applicationPayload = mCRecString;
  //    }
  //    Long origEventTimeStamp = rawEvent.getRheosHeader().getEventCreateTimestamp();
  //    abEventTimestamp = SojTimestamp.getSojTimestamp(origEventTimeStamp);
  //
  //    // for cal2.0 abeventtimestamp format change(from soj timestamp to EPOCH timestamp)
  //    String tstamp = rawEvent.getClientData().getTStamp();
  //    if (tstamp != null) {
  //      try {
  //        abEventTimestamp = Long.valueOf(rawEvent.getClientData().getTStamp());
  //        abEventTimestamp = SojTimestamp.getSojTimestamp(abEventTimestamp);
  //      } catch (NumberFormatException e) {
  //        abEventTimestamp = SojTimestamp.getSojTimestamp(origEventTimeStamp);
  //      }
  //    }
  //
  //    if (StringUtils.isNotBlank(map.get(Constants.P_TAG))) {
  //      pageId = map.get(Constants.P_TAG);
  //    }
  //
  //    if (pageId != null && !pageId.equals("5660")) {
  //      if (!StringUtils.isBlank(applicationPayload)) {
  //        // get mtsts from payload
  //        mtstsString =
  //            SOJURLDecodeEscape.decodeEscapes(
  //                SOJNVL.getTagValue(applicationPayload, Constants.TAG_MTSTS), '%');
  //
  //        // compare ab_event_timestamp and mtsts
  //        if (!StringUtils.isBlank(mtstsString) && mtstsString.trim().length() >= 21) {
  //          buffer
  //              .append(mtstsString, 0, 10)
  //              .append(" ")
  //              .append(mtstsString, 11, 19)
  //              .append(".")
  //              .append(mtstsString.substring(20));
  //          mtstsString = buffer.toString();
  //          buffer.setLength(0);
  //          try {
  //            if (mtstsString.endsWith("Z") || mtstsString.contains("T")) {
  //              mtstsString = mtstsString.replaceAll("T", " ")
  //                  .replaceAll("Z", "");
  //              eventTimestamp =
  //                  SojTimestamp.getSojTimestamp(formaterUtc.parseDateTime(mtstsString).getMillis());
  //            } else {
  //              eventTimestamp = SojTimestamp
  //                  .getSojTimestamp(formater.parseDateTime(mtstsString).getMillis());
  //            }
  //            interval = getMicroSecondInterval(eventTimestamp, abEventTimestamp);
  //            if (interval > Constants.UPPERLIMITMICRO || interval < Constants.LOWERLIMITMICRO) {
  //              eventTimestamp = abEventTimestamp;
  //            }
  //          } catch (Exception e) {
  //            log.error("Invalid mtsts: " + mtstsString);
  //            eventTimestamp = abEventTimestamp;
  //          }
  //        } else {
  //          eventTimestamp = abEventTimestamp;
  //        }
  //      } else {
  //        eventTimestamp = abEventTimestamp;
  //      }
  //    } else {
  //      eventTimestamp = abEventTimestamp;
  //    }
  //    rawEvent.setEventTimestamp(eventTimestamp);
  //  }

  // ignore second during comparing
  //  private Long getMicroSecondInterval(Long microts1, Long microts2) throws ParseException
  //  {
  //    Long v1, v2;
  //    v1 = dateMinsFormatter.parseDateTime(dateMinsFormatter.print(microts1 / 1000)).getMillis();
  //    v2 = dateMinsFormatter.parseDateTime(dateMinsFormatter.print(microts2 / 1000)).getMillis();
  //    return (v1 - v2) * 1000;
  //  }

  @Override
  public boolean isEndOfStream(RawEvent nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RawEvent> getProducedType() {
    return TypeInformation.of(RawEvent.class);
  }
}

