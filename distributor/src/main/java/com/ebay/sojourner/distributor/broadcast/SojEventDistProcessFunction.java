package com.ebay.sojourner.distributor.broadcast;

import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.util.ByteArrayUtils;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ebay.sojourner.common.constant.SojHeaders.*;
import static java.nio.charset.StandardCharsets.UTF_8;

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
    private final String DSS_ADS = "dss-ads";
    private final String DSS_GRO = "dss-gro";
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
            // 1. Ads MFE filter logic
            if (topicConfigMap.containsKey(DSS_ADS) && isEventForAdsMfe(sojEventWrapper)) {
                sojEventWrapper.setTopic(topicConfigMap.get(DSS_ADS));
                out.collect(sojEventWrapper);
            }

            // 2. DSS GRO filter logic
            if (topicConfigMap.containsKey(DSS_GRO) && isEventForDssGro(sojEventWrapper)) {
                sojEventWrapper.setTopic(topicConfigMap.get(DSS_GRO));
                out.collect(sojEventWrapper);
            }

            // 2. for EntryPage
            if (topicConfigMap.containsKey(ENTRY_PAGE) && isEventForEntryPage(sojEventWrapper)) {
                sojEventWrapper.setTopic(topicConfigMap.get(ENTRY_PAGE));
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
        Map<String, byte[]> headers = sojEventWrapper.getHeaders();
        return ByteArrayUtils.toBoolean(headers.get(IS_VALID_EVENT))
                && headers.get(SITE_ID) != null
                && SITE_IDS.contains(new String(headers.get(SITE_ID), UTF_8));
    }

    private boolean isEventForAdsMfe(RawSojEventWrapper sojEventWrapper) {
        return MFE_PAGE_IDS.contains(sojEventWrapper.getPageId())
                || sojEventWrapper.getHeaders().get(PLACEMENT_ID) != null;
    }

    private boolean isEventForEntryPage(RawSojEventWrapper sojEventWrapper) {
        Map<String, byte[]> headers = sojEventWrapper.getHeaders();
        return ByteArrayUtils.toBoolean(headers.get(EP));
    }
}
