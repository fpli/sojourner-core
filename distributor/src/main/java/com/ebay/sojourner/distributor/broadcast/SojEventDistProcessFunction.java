package com.ebay.sojourner.distributor.broadcast;

import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventHeader;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class SojEventDistProcessFunction extends
    BroadcastProcessFunction<RawSojEventWrapper, PageIdTopicMapping, RawSojEventWrapper> {

  private final MapStateDescriptor<Integer, PageIdTopicMapping> stateDescriptor;
  private final int ALL_PAGE = 0;
  private final Set<Integer> MFE_PAGE_IDS = Sets.newHashSet(
      2299321, 2062300, 2053742, 2053444, 2304207,
      2054032, 2317508, 2061037, 2063239, 2296363
  );
  private final Set<String> SITE_IDS = Sets.newHashSet("0", "2");
  private final String PVP_HSA_EFAM = "ONEPD";
  private final Set<String> PVP_HSA_EACTNS = Sets.newHashSet("EXPM", "VIEW", "ACTN");

  private final String SRCH_EFAM = "LST";
  private final String SRCH_EACTN = "SRCH";

  private final String DSS_ADS_MFE = "dss-ads-mfe";
  private final String DSS_ADS_PVP_HSA = "dss-ads-pvp-hsa";

  private final String DSS_GRO = "dss-gro";
  private final String SRCH = "lst-srch";
  private final String ENTRY_PAGE = "entryPage";
  private final Map<String, String> topicConfigMap = new HashMap<>();

  public SojEventDistProcessFunction(MapStateDescriptor<Integer, PageIdTopicMapping> descriptor,
                                     List<String> topicConfigs) {
    this.stateDescriptor = descriptor;
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
  public void processElement(RawSojEventWrapper sojEventWrapper, ReadOnlyContext ctx,
                             Collector<RawSojEventWrapper> out) throws Exception {
    ReadOnlyBroadcastState<Integer, PageIdTopicMapping> broadcastState =
        ctx.getBroadcastState(stateDescriptor);

    // distribute events based on pageid regardless event is bot or not
    int pageId = sojEventWrapper.getPageId();
    PageIdTopicMapping mapping = broadcastState.get(pageId);
    if (mapping != null) {
      for (String topic : mapping.getTopics()) {
        sojEventWrapper.setTopic(topic);
        out.collect(sojEventWrapper);
      }
    }

    if (sojEventWrapper.getBot() == 0) {
      // for nonbot sojevents, distribute based on complicated filtering logic
      // 1. DSS-Ads MFE filter logic
      if (topicConfigMap.containsKey(DSS_ADS_MFE) && isEventForDssAdsMfe(sojEventWrapper)) {
        sojEventWrapper.setTopic(topicConfigMap.get(DSS_ADS_MFE));
        out.collect(sojEventWrapper);
      }

      // 2. DSS-Ads PVP HSA filter logic
      if (topicConfigMap.containsKey(DSS_ADS_PVP_HSA)
          && isEventForDssAdsPvpHsa(sojEventWrapper)) {
        sojEventWrapper.setTopic(topicConfigMap.get(DSS_ADS_PVP_HSA));
        out.collect(sojEventWrapper);
      }

      // 3. DSS-GRO filter logic
      if (topicConfigMap.containsKey(DSS_GRO) && isEventForDssGro(sojEventWrapper)) {
        sojEventWrapper.setTopic(topicConfigMap.get(DSS_GRO));
        out.collect(sojEventWrapper);
      }

      // 4. for EntryPage
      if (topicConfigMap.containsKey(ENTRY_PAGE) && isEventForEntryPage(sojEventWrapper)) {
        sojEventWrapper.setTopic(topicConfigMap.get(ENTRY_PAGE));
        out.collect(sojEventWrapper);
      }

      // 5. for Search
      if (topicConfigMap.containsKey(SRCH) && isEventForSearch(sojEventWrapper)) {
        sojEventWrapper.setTopic(topicConfigMap.get(SRCH));
        out.collect(sojEventWrapper);
      }

    } else {
      // for bot sojevents, distribute all events if all_page(0) is set
      if (broadcastState.get(ALL_PAGE) != null) {
        for (String topic : broadcastState.get(ALL_PAGE).getTopics()) {
          sojEventWrapper.setTopic(topic);
          out.collect(sojEventWrapper);
        }
      }
    }
  }



  @Override
  public void processBroadcastElement(PageIdTopicMapping mapping, Context ctx,
                                      Collector<RawSojEventWrapper> out) throws Exception {
    log.info("process broadcast pageId topic mapping: {}", mapping);
    BroadcastState<Integer, PageIdTopicMapping> broadcastState =
        ctx.getBroadcastState(stateDescriptor);
    if (mapping.getTopics() == null) {
      // remove topic/pageid from state
      broadcastState.remove(mapping.getPageId());
    } else {
      broadcastState.put(mapping.getPageId(), mapping);
    }
  }

  private boolean isEventForDssGro(RawSojEventWrapper sojEventWrapper) {
    RawSojEventHeader headers = sojEventWrapper.getHeaders();
    return headers.isValidEvent()
        && SITE_IDS.contains(headers.getSiteId());
  }

  private boolean isEventForDssAdsMfe(RawSojEventWrapper sojEventWrapper) {
    return MFE_PAGE_IDS.contains(sojEventWrapper.getPageId())
        || sojEventWrapper.getHeaders().getPlmt() != null;
  }

  private boolean isEventForDssAdsPvpHsa(RawSojEventWrapper sojEventWrapper) {
    return PVP_HSA_EFAM.equals(sojEventWrapper.getHeaders().getEfam())
        && PVP_HSA_EACTNS.contains(sojEventWrapper.getHeaders().getEactn());
  }

  private boolean isEventForEntryPage(RawSojEventWrapper sojEventWrapper) {
    return sojEventWrapper.getHeaders().isEntryPage();
  }

  private boolean isEventForSearch(RawSojEventWrapper sojEventWrapper) {
    return SRCH_EFAM.equals(sojEventWrapper.getHeaders().getEfam())
        && SRCH_EACTN.equals(sojEventWrapper.getHeaders().getEactn());
  }
}
