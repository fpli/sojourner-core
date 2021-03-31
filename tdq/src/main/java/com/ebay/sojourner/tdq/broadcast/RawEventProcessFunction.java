package com.ebay.sojourner.tdq.broadcast;

import com.ebay.sojourner.common.model.*;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.SojUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RawEventProcessFunction extends
        BroadcastProcessFunction<RawEvent, TdqConfigMapping, RawEventMetrics> {

    private final MapStateDescriptor<String, TdqConfigMapping> stateDescriptor;

    public RawEventProcessFunction(MapStateDescriptor<String, TdqConfigMapping> descriptor) {
        this.stateDescriptor = descriptor;
    }

    @Override
    public void processElement(RawEvent rawEvent, ReadOnlyContext ctx,
                               Collector<RawEventMetrics> out) throws Exception {
        ReadOnlyBroadcastState<String, TdqConfigMapping> broadcastState =
                ctx.getBroadcastState(stateDescriptor);
        RawEventMetrics rawEventMetrics = new RawEventMetrics();
        rawEventMetrics.setGuid(SojUtils.getTagValueStr(rawEvent, Constants.G_TAG));
        for (Map.Entry<String, TdqConfigMapping> entry : broadcastState.immutableEntries()) {
            calculateMetrics(rawEventMetrics, rawEvent, entry.getValue(), entry.getKey());
        }
        out.collect(rawEventMetrics);
    }

    @Override
    public void processBroadcastElement(TdqConfigMapping mapping, Context ctx,
                                        Collector<RawEventMetrics> out) throws Exception {
        log.info("process broadcast tdq config mapping: {}", mapping);
        BroadcastState<String, TdqConfigMapping> broadcastState =
                ctx.getBroadcastState(stateDescriptor);
        MetricType metricType = mapping.getMetricType();
        StringBuilder metricDesc = new StringBuilder();
        if (metricType != null) {
            metricDesc.append(metricType.name());
        }
        metricDesc.append(Constants.METRIC_DEL).append(mapping.getMetricName());
        broadcastState.put(metricDesc.toString(), mapping);
    }

    private void calculateMetrics(RawEventMetrics rawEventMetrics, RawEvent rawEvent,
                                  TdqConfigMapping tdqConfigMapping, String metricKey) {
        MetricType metricType = tdqConfigMapping.getMetricType();
        switch (metricType) {
            case TAG_MISS_CNT:
                calculateTagMissingCntMetrics(rawEventMetrics, rawEvent,
                        tdqConfigMapping, metricKey);
                break;
            case TAG_SUM:
                calculateTagSumMetrics(rawEventMetrics, rawEvent, tdqConfigMapping, metricKey);
                break;
            case PAGE_CNT:
                calculatePageCntMetrics(rawEventMetrics, rawEvent, tdqConfigMapping, metricKey);
                break;
            case TRANSFORM_ERROR:
                calculateTransFormerErrorMetrics(rawEventMetrics, rawEvent, tdqConfigMapping,
                        metricKey);
                break;
            case TOTAL_CNT:
                calculateTotalCntMetrics(rawEventMetrics, rawEvent, tdqConfigMapping,
                        metricKey);
                break;
            default:
                break;
        }
    }

    private void calculateTagMissingCntMetrics(RawEventMetrics rawEventMetrics, RawEvent rawEvent,
                                               TdqConfigMapping tdqConfigMapping,
                                               String metricKey) {
        Integer pageId = SojUtils.getPageId(rawEvent);
        Integer siteId = SojUtils.getSiteId(rawEvent);
        if (pageId != null) {
            String pageFamily = SojUtils.getPageFmly(pageId);
            boolean ifCountIn = SojUtils.checkIfCountIn(pageId);
            if (ifCountIn && tdqConfigMapping.getPageFamilys() != null
                    && tdqConfigMapping.getPageFamilys().contains(pageFamily)) {
                StringBuilder domain = new StringBuilder(pageFamily).append(Constants.DOMAIN_DEL)
                        .append(siteId == null ? "null" : siteId);
                Set<String> tags = tdqConfigMapping.getTags();
                TagMissingCntMetrics tagMissingCntMetrics = new TagMissingCntMetrics();
                tagMissingCntMetrics.setMetricName(tdqConfigMapping.getMetricName());
                tagMissingCntMetrics.setMetricType(tdqConfigMapping.getMetricType());
                tagMissingCntMetrics.getPageFamilySet().add(pageFamily);
                if (CollectionUtils.isNotEmpty(tags)) {
                    Map<String, Long> tagWithCnt = new HashMap<>();
                    for (String tagName : tags) {
                        if (!"u".equals(tagName)) {
                            long tagCnt = SojUtils.getTagMissingCnt(rawEvent, tagName);
                            if (tagCnt != 0) {
                                tagWithCnt.put(tagName, tagCnt);
                            }
                        } else {
                            long tagCnt = SojUtils.getTagMissingCnt(rawEvent, tagName);
                            if (tagCnt != 0) {
                                tagWithCnt.put(tagName, tagCnt);
                            }
                        }
                    }
                    tagMissingCntMetrics.getTagCntMap().put(domain.toString(), tagWithCnt);
                }
                rawEventMetrics.getTagMissingCntMetricsMap().put(metricKey, tagMissingCntMetrics);
            }
        }
    }

    private void calculateTagSumMetrics(RawEventMetrics rawEventMetrics, RawEvent rawEvent,
                                        TdqConfigMapping tdqConfigMapping, String metricKey) {
        Integer pageId = SojUtils.getPageId(rawEvent);
        Integer siteId = SojUtils.getSiteId(rawEvent);
        String pageFamily = SojUtils.getPageFmly(pageId);
        StringBuilder domain = new StringBuilder(pageFamily).append(Constants.DOMAIN_DEL)
                .append(siteId == null ? "null" : siteId);
        Set<String> tags = tdqConfigMapping.getTags();
        TagSumMetrics tagSumMetrics = new TagSumMetrics();
        tagSumMetrics.setMetricName(tdqConfigMapping.getMetricName());
        tagSumMetrics.setMetricType(tdqConfigMapping.getMetricType());
        if (CollectionUtils.isNotEmpty(tags)) {
            Map<String, Double> tagWithCnt = new HashMap();
            for (String tagName : tags) {
                double tagValue = SojUtils.getTagValue(rawEvent, tagName);
                tagWithCnt.put(tagName, tagValue);
            }
            tagSumMetrics.getTagSumMap().put(domain.toString(), tagWithCnt);
        }
        rawEventMetrics.getTagSumMetricsMap().put(metricKey, tagSumMetrics);
    }

    private void calculatePageCntMetrics(RawEventMetrics rawEventMetrics, RawEvent rawEvent,
                                         TdqConfigMapping tdqConfigMapping, String metricKey) {
        Integer pageId = SojUtils.getPageId(rawEvent);
        if (pageId != null) {
            Integer siteId = SojUtils.getSiteId(rawEvent);
            String pageFamily = SojUtils.getPageFmly(pageId);
            StringBuilder domain = new StringBuilder(pageFamily).append(Constants.DOMAIN_DEL)
                    .append(siteId == null ? "null" : siteId);
            Set<Integer> pageIds = tdqConfigMapping.getPageIds();
            PageCntMetrics pageCntMetrics = new PageCntMetrics();
            pageCntMetrics.setMetricName(tdqConfigMapping.getMetricName());
            pageCntMetrics.setMetricType(tdqConfigMapping.getMetricType());
            if (CollectionUtils.isNotEmpty(pageIds)) {
                Map<Integer, Long> pageWithCnt = new HashMap();
                if (pageIds.contains(pageId)) {
                    pageWithCnt.put(pageId, 1L);
                }
                pageCntMetrics.getPageCntMap().put(domain.toString(), pageWithCnt);
            }
            rawEventMetrics.getPageCntMetricsMap().put(metricKey, pageCntMetrics);
        }
    }

    private void calculateTransFormerErrorMetrics(RawEventMetrics rawEventMetrics,
                                                  RawEvent rawEvent,
                                                  TdqConfigMapping tdqConfigMapping,
                                                  String metricKey) {
        Integer pageId = SojUtils.getPageId(rawEvent);
        Integer siteId = SojUtils.getSiteId(rawEvent);
        String pageFamily = SojUtils.getPageFmly(pageId);
        StringBuilder domain = new StringBuilder(pageFamily).append(Constants.DOMAIN_DEL)
                .append(siteId == null ? "null" : siteId);
        Set<String> tags = tdqConfigMapping.getTags();
        TransformErrorMetrics transformErrorMetrics = new TransformErrorMetrics();
        transformErrorMetrics.setMetricName(tdqConfigMapping.getMetricName());
        transformErrorMetrics.setMetricType(tdqConfigMapping.getMetricType());
        if (CollectionUtils.isNotEmpty(tags)) {
            Map<String, Long> tagWithCnt = new HashMap();
            for (String tagName : tags) {
                String[] tagKV = tagName.split("-");
                long cnt = 0L;
                String tagValue = SojUtils.getTagValueStr(rawEvent, tagKV[0]);
                if ("u".equals(tagKV[0])) {
                    cnt = SojUtils.checkFormatForU(tagKV[1], tagValue);
                } else {
                    cnt = SojUtils.checkFormat(tagKV[1], tagValue);
                }
                if (cnt != 0) {
                    tagWithCnt.put(tagKV[0], cnt);
                }
            }
            transformErrorMetrics.getTagErrorCntMap().put(domain.toString(), tagWithCnt);
        }
        rawEventMetrics.getTransformErrorMetricsMap().put(metricKey, transformErrorMetrics);
    }

    private void calculateTotalCntMetrics(RawEventMetrics rawEventMetrics, RawEvent rawEvent,
                                          TdqConfigMapping tdqConfigMapping, String metricKey) {
        Integer pageId = SojUtils.getPageId(rawEvent);
        if (pageId != null) {
            Integer siteId = SojUtils.getSiteId(rawEvent);
            String pageFamily = SojUtils.getPageFmly(pageId);
            boolean ifCountIn = SojUtils.checkIfCountIn(pageId);
            StringBuilder domain = new StringBuilder(pageFamily).append(Constants.DOMAIN_DEL)
                    .append(siteId == null ? "null" : siteId);
            TotalCntMetrics totalCntMetrics = new TotalCntMetrics();
            totalCntMetrics.setMetricName(tdqConfigMapping.getMetricName());
            totalCntMetrics.setMetricType(tdqConfigMapping.getMetricType());
            totalCntMetrics.getTotalCntMap().put(domain.toString(), 1L);
            if (ifCountIn && tdqConfigMapping.getPageFamilys() != null
                    && tdqConfigMapping.getPageFamilys().contains(pageFamily)) {
                totalCntMetrics.getTotalCntItmMap().put(domain.toString(), 1L);
            }
            rawEventMetrics.getTotalCntMetricsMap().put(metricKey, totalCntMetrics);
        }
    }

}