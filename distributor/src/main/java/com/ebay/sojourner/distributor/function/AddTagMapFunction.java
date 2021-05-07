package com.ebay.sojourner.distributor.function;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.AGENT_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.BOTT_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.CBRND_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.CLIENT_IP_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.CORRID_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.CURRENT_IMPR_ID_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.DD_BF_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.DD_BV_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.DD_DC_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.DD_D_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.DD_OSV_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.DD_OS_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EVENT_TS_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.FORWARDED_FOR_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.IFRM_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.JS_EV_MAK_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.NODE_ID_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.PAGE_NAME_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.PAYLOAD_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.RDT_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.REFERER_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.REMOTE_IP_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.REQUEST_GUID_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.RHEOS_UPSTREAM_CREATE_TS_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.RHEOS_UPSTREAM_SEND_TS_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.RLOGID_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.RV_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.TIMESTAMP_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.TPOOL_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.TTYPE_TAG;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.URL_QUERY_STRING_TAG;

import com.ebay.sojourner.common.constant.ApplicationPayloadTags;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class AddTagMapFunction extends RichMapFunction<SojEvent, RawSojEventWrapper> {

  private transient KafkaSerializer<SojEvent> serializer;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    serializer = new AvroKafkaSerializer<>(SojEvent.getClassSchema());
  }

  @Override
  public RawSojEventWrapper map(SojEvent event) throws Exception {
    // add JetStream backward compatible tags in 'applicationPayload' field
    try {
      Map<String, String> applicationPayload = event.getApplicationPayload();
      // generate UUID for per sojevent
      applicationPayload.put(ApplicationPayloadTags.UUID, UUID.randomUUID().toString());
      // add tags
      if (event.getClientData().containsKey("TStamp")) {
        applicationPayload.put(EVENT_TS_TAG, event.getClientData().get("TStamp"));
      }
      if (event.getClientData().containsKey("ForwardedFor")) {
        applicationPayload.put(FORWARDED_FOR_TAG, event.getClientData().get("ForwardedFor"));
      }
      if (event.getClientData().containsKey("TPool")) {
        applicationPayload.put(TPOOL_TAG, event.getClientData().get("TPool"));
      }
      if (event.getClientData().containsKey("TType")) {
        applicationPayload.put(TTYPE_TAG, event.getClientData().get("TType"));
      }
      if (event.getClientData().containsKey("corrId")) {
        applicationPayload.put(CORRID_TAG, event.getClientData().get("corrId"));
      }
      if (event.getClientData().containsKey("nodeId")) {
        applicationPayload.put(NODE_ID_TAG, event.getClientData().get("nodeId"));
      }
      if (event.getClientData().containsKey("reqeustGuid")) {
        applicationPayload.put(REQUEST_GUID_TAG, event.getClientData().get("reqeustGuid"));
      }

      // Referer tag is always there
      applicationPayload.put(REFERER_TAG, event.getClientData().getOrDefault("Referer", ""));

      if (StringUtils.isNotBlank(event.getAgentInfo())) {
        applicationPayload.put(AGENT_TAG, event.getAgentInfo());
      }
      if (StringUtils.isNotBlank(event.getUrlQueryString())) {
        applicationPayload.put(PAYLOAD_TAG, event.getUrlQueryString());
        applicationPayload.put(URL_QUERY_STRING_TAG, event.getUrlQueryString());
      }
      if (StringUtils.isNotBlank(event.getRemoteIP())) {
        applicationPayload.put(REMOTE_IP_TAG, event.getRemoteIP());
      }
      if (event.getBot() != null) {
        applicationPayload.put(BOTT_TAG, String.valueOf(event.getBot()));
      }
      if (StringUtils.isNotBlank(event.getCobrand())) {
        applicationPayload.put(CBRND_TAG, event.getCobrand());
      }
      if (StringUtils.isNotBlank(event.getClientIP())) {
        applicationPayload.put(CLIENT_IP_TAG, event.getClientIP());
      }
      if (event.getCurrentImprId() != null) {
        applicationPayload.put(CURRENT_IMPR_ID_TAG, String.valueOf(event.getCurrentImprId()));
      }
      if (StringUtils.isNotBlank(event.getBrowserFamily())) {
        applicationPayload.put(DD_BF_TAG, event.getBrowserFamily());
      }
      if (StringUtils.isNotBlank(event.getBrowserVersion())) {
        applicationPayload.put(DD_BV_TAG, event.getBrowserVersion());
      }
      if (StringUtils.isNotBlank(event.getDeviceFamily())) {
        applicationPayload.put(DD_D_TAG, event.getDeviceFamily());
      }
      if (StringUtils.isNotBlank(event.getDeviceType())) {
        applicationPayload.put(DD_DC_TAG, event.getDeviceType());
      }
      if (StringUtils.isNotBlank(event.getOsFamily())) {
        applicationPayload.put(DD_OS_TAG, event.getOsFamily());
      }
      if (StringUtils.isNotBlank(event.getEnrichedOsVersion())) {
        applicationPayload.put(DD_OSV_TAG, event.getEnrichedOsVersion());
      }
      if (event.getIframe() != null) {
        applicationPayload.put(IFRM_TAG, String.valueOf(event.getIframe()));
      }
      if (StringUtils.isNotBlank(event.getGuid())) {
        applicationPayload.put(JS_EV_MAK_TAG, event.getGuid());
      }
      if (StringUtils.isNotBlank(event.getPageName())) {
        applicationPayload.put(PAGE_NAME_TAG, event.getPageName());
      }
      if (event.getRdt() != null) {
        applicationPayload.put(RDT_TAG, String.valueOf(event.getRdt()));
      }
      if (StringUtils.isNotBlank(event.getRlogid())) {
        applicationPayload.put(RLOGID_TAG, event.getRlogid());
      }
      if (event.getRv() != null) {
        applicationPayload.put(RV_TAG, String.valueOf(event.getRv()));
      }
      if (event.getEventTimestamp() != null) {
        applicationPayload.put(TIMESTAMP_TAG, String.valueOf(event.getEventTimestamp()));
      }
      if (event.getRheosHeader().getEventCreateTimestamp() != null) {
        applicationPayload.put(RHEOS_UPSTREAM_CREATE_TS_TAG,
                               String.valueOf(event.getEventTimestamp()));
      }
      if (event.getRheosHeader().getEventSentTimestamp() != null) {
        applicationPayload.put(RHEOS_UPSTREAM_SEND_TS_TAG,
                               String.valueOf(System.currentTimeMillis()));
      }

      byte[] payloads = serializer.encodeValue(event);
      return new RawSojEventWrapper(event.getGuid(), event.getPageId(), null, payloads);
    } catch (Exception ex) {
      log.error("Failed to add tag", ex);
      throw new RuntimeException(ex);
    }
  }
}
