package com.ebay.sojourner.distributor.function;

import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.flink.connector.kafka.AvroKafkaSerializer;
import com.ebay.sojourner.flink.connector.kafka.KafkaSerializer;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SojEventFilterProcessFunction extends ProcessFunction<SojEvent, RawSojEventWrapper> {

  private transient KafkaSerializer<SojEvent> serializer;
  private final Set<Integer> MFE_PAGE_IDS = Sets.newHashSet(
      2299321, 2062300, 2053742, 2053444, 2304207,
      2054032, 2317508, 2061037, 2063239, 2296363
  );
  private final Set<String> SITE_IDS = Sets.newHashSet("0", "2");
  private final String ADS_MFE = "ads-mfe";
  private final String DSS_GRO = "dss-gro";
  private final Map<String, String> topicConfigMap = new HashMap<>();

  public SojEventFilterProcessFunction(List<String> topicConfigs) {
    if (topicConfigs != null) {
      for (String topicConfig : topicConfigs) {
        String[] configStr = topicConfig.split(":");
        if (configStr.length == 2) {
          topicConfigMap.put(configStr[0], configStr[1]);
        }
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    serializer = new AvroKafkaSerializer<>(SojEvent.getClassSchema());
  }

  @Override
  public void processElement(SojEvent event, Context ctx,
                             Collector<RawSojEventWrapper> out) throws Exception {

    // only process nonbot sojevents
    if (event.getBot() == 0) {
      byte[] payloads = null;

      // 1. Ads MFE filter logic
      if (topicConfigMap.containsKey(ADS_MFE) && isEventForAdsMfe(event)) {
        payloads = payloads == null ? serializer.encodeValue(event) : payloads;
        out.collect(new RawSojEventWrapper(
            event.getGuid(), event.getPageId(), topicConfigMap.get(ADS_MFE), payloads));
      }

      // 2. DSS GRO filter logic
      if (topicConfigMap.containsKey(DSS_GRO) && isEventForDssGro(event)) {
        payloads = payloads == null ? serializer.encodeValue(event) : payloads;
        out.collect(new RawSojEventWrapper(
            event.getGuid(), event.getPageId(), topicConfigMap.get(DSS_GRO), payloads));
      }
    }
  }

  private boolean isEventForDssGro(SojEvent event) {
    return event.getRdt().equals(0)
        && event.getBot().equals(0)
        && !event.getIframe()
        && SITE_IDS.contains(event.getSiteId())
        && event.getPageId() != null;
  }

  private boolean isEventForAdsMfe(SojEvent event) {
    return MFE_PAGE_IDS.contains(event.getPageId())
        || event.getApplicationPayload().containsKey("plmt");
  }
}
