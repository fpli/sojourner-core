package com.ebay.sojourner.distributor.function;

import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EACTN;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.EFAM;
import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.PLMT;
import static com.ebay.sojourner.common.constant.SojHeaders.EP;

import com.ebay.sojourner.common.model.RawSojEventHeader;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.ByteArrayUtils;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import java.nio.ByteBuffer;
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
    RawSojEventHeader header = new RawSojEventHeader();
    header.setValidEvent(isValidEvent(event));
    header.setPlmt(event.getApplicationPayload().get(PLMT));
    header.setEfam(event.getApplicationPayload().get(EFAM));
    header.setEactn(event.getApplicationPayload().get(EACTN));
    header.setSiteId(event.getSiteId());
    header.setEntryPage(isEntryPage(event));

    return new RawSojEventWrapper(event.getGuid(), event.getPageId(), event.getBot(), header,
                                  null, payloads);
  }

  private boolean isValidEvent(SojEvent event) {
    return event.getRdt().equals(0) && !event.getIframe();
  }

  private boolean isEntryPage(SojEvent event) {
    Map<String, ByteBuffer> sojHeader = event.getSojHeader();
    if (sojHeader.containsKey(EP)) {
      return ByteArrayUtils.toBoolean(sojHeader.get(EP).array());
    }
    return false;
  }

}
