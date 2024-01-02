package com.ebay.sojourner.distributor.schema.bullseye;

import com.ebay.sojourner.common.constant.KafkaMessageHeaders;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.common.KafkaHeaderUtils;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaDeserializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaDeserializer;
import com.google.common.collect.Sets;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

@Slf4j
public class BullseyeSojEventDeserializationSchema
    implements KafkaDeserializationSchema<SojEvent> {

  private transient KafkaDeserializer<SojEvent> deserializer;

  private final Set<Integer> BULLSEYE_PAGE_IDS = Sets.newHashSet(
      2376473, 2493971, 2493972, 2493975, 2493976, 2508507, 2500857, 2503558,
      2368482, 2368479, 2368478, 4852, 2051246, 2508691, 2103899, 2266111, 2509140, 2045573,
      2046732, 2047936, 2051457, 2053742, 2351460, 2364840, 2047675, 2047935, 2056116, 2349624,
      5408, 2501496, 2322090, 2376289, 3418065, 4429486);

  @Override
  public void open(InitializationContext context) throws Exception {
    log.info("Initialize KafkaDeserializer for SojEvent.");
    KafkaDeserializationSchema.super.open(context);
    deserializer = new AvroKafkaDeserializer<>(SojEvent.class);
  }

  @Override
  public boolean isEndOfStream(SojEvent nextElement) {
    return false;
  }

  @Override
  public SojEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    Headers headers = record.headers();
    Integer pageId = KafkaHeaderUtils.getInt(KafkaMessageHeaders.PAGE_ID, headers);
    // only keep the necessary pageids
    if (pageId != null && BULLSEYE_PAGE_IDS.contains(pageId)) {
      return deserializer.decodeValue(record.value());
    }
    return null;
  }

  @Override
  public TypeInformation<SojEvent> getProducedType() {
    return TypeInformation.of(SojEvent.class);
  }

}
