package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.ByteArrayUtils;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.ebay.sojourner.common.constant.SojHeaders.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SojEventToRawSojEventWrapperMapFunction
    extends RichMapFunction<SojEvent, RawSojEventWrapper> {

  private transient KafkaSerializer<SojEvent> serializer;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    serializer = new AvroKafkaSerializer<>(SojEvent.getClassSchema());
  }

  @Override
  public RawSojEventWrapper map(SojEvent event) throws Exception {
    byte[] payloads = serializer.encodeValue(event);
    Map<String, byte[]> headers = new HashMap<>();
    headers.put(IS_VALID_EVENT, ByteArrayUtils.fromBoolean(isValidEvent(event)));
    String plmt = event.getApplicationPayload().get("plmt");
    headers.put(PLACEMENT_ID, plmt == null ? null : plmt.getBytes(UTF_8));
    String siteId = event.getSiteId();
    headers.put(SITE_ID, siteId == null ? null : siteId.getBytes(UTF_8));
    Map<String, ByteBuffer> sojHeaderMap= event.getSojHeader();
    if(MapUtils.isNotEmpty(sojHeaderMap)){
      headers.put(EP, sojHeaderMap.get(EP).array());
    }
    headers.put(SITE_ID, siteId == null ? null : siteId.getBytes(UTF_8));
    return new RawSojEventWrapper(event.getGuid(), event.getPageId(), event.getBot(), headers,
                                  null, payloads);
  }

  private boolean isValidEvent(SojEvent event) {
    return event.getRdt().equals(0) && !event.getIframe();
  }

}
