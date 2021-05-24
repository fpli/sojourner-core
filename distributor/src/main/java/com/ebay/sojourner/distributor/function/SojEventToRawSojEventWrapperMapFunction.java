package com.ebay.sojourner.distributor.function;

import static com.ebay.sojourner.common.constant.SojHeaders.IS_VALID_EVENT;
import static com.ebay.sojourner.common.constant.SojHeaders.PLACEMENT_ID;
import static com.ebay.sojourner.common.constant.SojHeaders.SITE_ID;

import com.ebay.sojourner.common.util.ByteArrayUtils;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

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
    headers.put(PLACEMENT_ID, plmt == null ? null : plmt.getBytes(StandardCharsets.UTF_8));
    String siteId = event.getSiteId();
    headers.put(SITE_ID, siteId == null ? null : siteId.getBytes(StandardCharsets.UTF_8));

    return new RawSojEventWrapper(event.getGuid(), event.getPageId(), event.getBot(), headers,
                                  null, payloads);
  }

  private boolean isValidEvent(SojEvent event) {
    return event.getRdt().equals(0) && !event.getIframe();
  }

}
