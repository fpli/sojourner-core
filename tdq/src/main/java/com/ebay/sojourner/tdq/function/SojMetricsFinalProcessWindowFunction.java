package com.ebay.sojourner.tdq.function;

import com.ebay.sojourner.common.model.*;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.tdq.metrics.SojMetricsGauge;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Map.Entry;

public class SojMetricsFinalProcessWindowFunction<T extends TdqMetrics> extends
        ProcessWindowFunction<T, T, Tuple, TimeWindow> {
    private static final Map<String, Gauge> domainMetricGuageMap = new ConcurrentHashMap<>();
    private static final Map<String, Object> domainMetricValueMap = new ConcurrentHashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<T> elements,
                        Collector<T> out) {
        T sojMetrics = elements.iterator().next();
        collectSojMetrics(sojMetrics);
        //        out.collect(null);
    }

    private void collectSojMetrics(T sojMetrics) {
        if (sojMetrics instanceof PageCntMetrics) {
            collectPageCntMetrics((PageCntMetrics) sojMetrics);
        } else if (sojMetrics instanceof TagMissingCntMetrics) {
            collectTagMissingCntMetrics((TagMissingCntMetrics) sojMetrics);
        } else if (sojMetrics instanceof TagSumMetrics) {
            collectTagSumMetrics((TagSumMetrics) sojMetrics);
        } else if (sojMetrics instanceof TransformErrorMetrics) {
            collectTransformErrorMetrics((TransformErrorMetrics) sojMetrics);
        } else if (sojMetrics instanceof TotalCntMetrics) {
            collectTotalCntMetrics((TotalCntMetrics) sojMetrics);
        }
    }

    private void collectTagMissingCntMetrics(TagMissingCntMetrics tagMissingCntMetrics) {
        String metricKey = constructMetricKey(tagMissingCntMetrics);
        Map<String, Map<String, Long>> domainTagMissingCntMap
                = tagMissingCntMetrics.getTagCntMap();
        for (Entry<String, Map<String, Long>> domainTagMissingCntEntry
                : domainTagMissingCntMap.entrySet()) {
            String domain = domainTagMissingCntEntry.getKey();
            String[] domainList = domain.split(Constants.DOMAIN_DEL);
            Map<String, Long> tagMissingCntMap = domainTagMissingCntEntry.getValue();
            for (Entry<String, Long> tagMissingCntEntry : tagMissingCntMap.entrySet()) {
                String tagName = tagMissingCntEntry.getKey();
                Long tagCount = tagMissingCntEntry.getValue();
                StringBuilder sb = new StringBuilder(Constants.SOJ_METRICS_GROUP)
                        .append(metricKey).append(domain)
                        .append(tagMissingCntMetrics.getEventTime()).append(tagName);
                if (!domainMetricValueMap.containsKey(sb.toString())) {
                    domainMetricValueMap.put(sb.toString(), tagCount);
                    Gauge gauge = getRuntimeContext()
                            .getMetricGroup().addGroup(Constants.SOJ_METRICS_GROUP)
                            .addGroup(Constants.SOJ_METRIC_TYPE,
                                    tagMissingCntMetrics.getMetricType().toString())
                            .addGroup(Constants.SOJ_METRIC_NAME,
                                    tagMissingCntMetrics.getMetricName())
                            .addGroup(Constants.SOJ_PGAE_FAMILY, domainList[0])
                            .addGroup(Constants.SOJ_SITE_ID, domainList[1])
                            .addGroup(Constants.SOJ_EVENT_TIME,
                                    String.valueOf(tagMissingCntMetrics.getEventTime()))
                            .addGroup(Constants.SOJ_TAG_NAME,
                                    tagName.replace("|", "-"))
                            .gauge(Constants.TAG_MISSING_CNT_METRICS,
                                    new SojMetricsGauge(domainMetricValueMap,
                                            sb.toString()));
                    domainMetricGuageMap.put(sb.toString(), gauge);
                } else {
                    domainMetricValueMap.put(sb.toString(), tagCount);
                }
            }
        }

    }

    private void collectTagSumMetrics(TagSumMetrics tagSumMetrics) {
        String metricKey = constructMetricKey(tagSumMetrics);
        Map<String, Map<String, Double>> domainTagSumMap = tagSumMetrics.getTagSumMap();
        for (Entry<String, Map<String, Double>> domainTagSumEntry
                : domainTagSumMap.entrySet()) {
            String domain = domainTagSumEntry.getKey();
            String[] domainList = domain.split(Constants.DOMAIN_DEL);
            Map<String, Double> tagSumMap = domainTagSumEntry.getValue();
            for (Entry<String, Double> tagSumEntry : tagSumMap.entrySet()) {
                String tagName = tagSumEntry.getKey();
                Double tagCount = tagSumEntry.getValue();
                StringBuilder sb = new StringBuilder(Constants.SOJ_METRICS_GROUP)
                        .append(metricKey).append(domain)
                        .append(tagSumMetrics.getEventTime()).append(tagName);
                if (!domainMetricValueMap.containsKey(sb.toString())) {
                    domainMetricValueMap.put(sb.toString(), tagCount);
                    Gauge gauge = getRuntimeContext()
                            .getMetricGroup().addGroup(Constants.SOJ_METRICS_GROUP)
                            .addGroup(Constants.SOJ_METRIC_TYPE,
                                    tagSumMetrics.getMetricType().toString())
                            .addGroup(Constants.SOJ_METRIC_NAME,
                                    tagSumMetrics.getMetricName())
                            .addGroup(Constants.SOJ_PGAE_FAMILY, domainList[0])
                            .addGroup(Constants.SOJ_SITE_ID, domainList[1])
                            .addGroup(Constants.SOJ_EVENT_TIME,
                                    String.valueOf(tagSumMetrics.getEventTime()))
                            .addGroup(Constants.SOJ_TAG_NAME, tagName)
                            .gauge(Constants.TAG_SUM_METRICS,
                                    new SojMetricsGauge(domainMetricValueMap,
                                            sb.toString()));
                    domainMetricGuageMap.put(sb.toString(), gauge);
                } else {
                    domainMetricValueMap.put(sb.toString(), tagCount);
                }
            }
        }
    }

    private void collectPageCntMetrics(PageCntMetrics pageCntMetrics) {
        String metricKey = constructMetricKey(pageCntMetrics);
        Map<String, Map<Integer, Long>> domainPageCntMap = pageCntMetrics.getPageCntMap();
        for (Entry<String, Map<Integer, Long>> domainPageCntEntry :
                domainPageCntMap.entrySet()) {
            String domain = domainPageCntEntry.getKey();
            String[] domainList = domain.split(Constants.DOMAIN_DEL);
            Map<Integer, Long> pageCntMap = domainPageCntEntry.getValue();
            for (Entry<Integer, Long> pageCntEntry : pageCntMap.entrySet()) {
                Integer pageId = pageCntEntry.getKey();
                Long pageCnt = pageCntEntry.getValue();
                StringBuilder sb = new StringBuilder(Constants.SOJ_METRICS_GROUP)
                        .append(metricKey).append(domain)
                        .append(pageCntMetrics.getEventTime()).append(pageId);
                if (!domainMetricValueMap.containsKey(sb.toString())) {
                    domainMetricValueMap.put(sb.toString(), pageCnt);
                    Gauge gauge = getRuntimeContext()
                            .getMetricGroup().addGroup(Constants.SOJ_METRICS_GROUP)
                            .addGroup(Constants.SOJ_METRIC_TYPE,
                                    pageCntMetrics.getMetricType().toString())
                            .addGroup(Constants.SOJ_METRIC_NAME,
                                    pageCntMetrics.getMetricName())
                            .addGroup(Constants.SOJ_PGAE_FAMILY, domainList[0])
                            .addGroup(Constants.SOJ_SITE_ID, domainList[1])
                            .addGroup(Constants.SOJ_EVENT_TIME,
                                    String.valueOf(pageCntMetrics.getEventTime()))
                            .addGroup(Constants.SOJ_PAGE_ID, String.valueOf(pageId))
                            .gauge(Constants.PAGE_CNT_METRICS,
                                    new SojMetricsGauge(domainMetricValueMap,
                                            sb.toString()));
                    domainMetricGuageMap.put(sb.toString(), gauge);

                } else {
                    domainMetricValueMap.put(sb.toString(), pageCnt);
                }
            }
        }
    }

    private void collectTotalCntMetrics(TotalCntMetrics totalCntMetrics) {
        String metricKey = constructMetricKey(totalCntMetrics);
        Map<String, Long> domainTotalCntMap = totalCntMetrics.getTotalCntMap();
        for (Entry<String, Long> domainTotalCntEntry :
                domainTotalCntMap.entrySet()) {
            String domain = domainTotalCntEntry.getKey();
            String[] domainList = domain.split(Constants.DOMAIN_DEL);
            Long totalCnt = domainTotalCntEntry.getValue();
            StringBuilder sb = new StringBuilder(Constants.SOJ_METRICS_GROUP)
                    .append(metricKey).append(domain).append(Constants.SOJ_METRICS_TOTAL)
                    .append(totalCntMetrics.getEventTime());
            if (!domainMetricValueMap.containsKey(sb.toString())) {
                domainMetricValueMap.put(sb.toString(), totalCnt);
                Gauge gauge = getRuntimeContext()
                        .getMetricGroup().addGroup(Constants.SOJ_METRICS_GROUP)
                        .addGroup(Constants.SOJ_METRIC_TYPE,
                                totalCntMetrics.getMetricType().toString())
                        .addGroup(Constants.SOJ_METRIC_NAME,
                                totalCntMetrics.getMetricName())
                        .addGroup(Constants.SOJ_PGAE_FAMILY, domainList[0])
                        .addGroup(Constants.SOJ_SITE_ID, domainList[1])
                        .addGroup(Constants.SOJ_EVENT_TIME,
                                String.valueOf(totalCntMetrics.getEventTime()))
                        .gauge(Constants.TOTAL_CNT_METRICS,
                                new SojMetricsGauge(domainMetricValueMap,
                                        sb.toString()));
                domainMetricGuageMap.put(sb.toString(), gauge);

            } else {
                domainMetricValueMap.put(sb.toString(), totalCnt);
            }
        }

        Map<String, Long> domainTotalCntItmMap = totalCntMetrics.getTotalCntItmMap();
        for (Entry<String, Long> domainTotalCntItmEntry :
                domainTotalCntItmMap.entrySet()) {
            String domain = domainTotalCntItmEntry.getKey();
            String[] domainList = domain.split(Constants.DOMAIN_DEL);
            Long totalCnt = domainTotalCntItmEntry.getValue();
            StringBuilder sb = new StringBuilder(Constants.SOJ_METRICS_GROUP)
                    .append(metricKey).append(domain).append(Constants.SOJ_METRICS_TOTAL_ITM)
                    .append(totalCntMetrics.getEventTime());
            if (!domainMetricValueMap.containsKey(sb.toString())) {
                domainMetricValueMap.put(sb.toString(), totalCnt);
                Gauge gauge = getRuntimeContext()
                        .getMetricGroup().addGroup(Constants.SOJ_METRICS_GROUP)
                        .addGroup(Constants.SOJ_METRIC_TYPE,
                                totalCntMetrics.getMetricType().toString())
                        .addGroup(Constants.SOJ_METRIC_NAME,
                                totalCntMetrics.getMetricName())
                        .addGroup(Constants.SOJ_PGAE_FAMILY, domainList[0])
                        .addGroup(Constants.SOJ_SITE_ID, domainList[1])
                        .addGroup(Constants.SOJ_EVENT_TIME,
                                String.valueOf(totalCntMetrics.getEventTime()))
                        .gauge(Constants.TOTAL_CNT_ITM_METRICS,
                                new SojMetricsGauge(domainMetricValueMap,
                                        sb.toString()));
                domainMetricGuageMap.put(sb.toString(), gauge);

            } else {
                domainMetricValueMap.put(sb.toString(), totalCnt);
            }
        }
    }

    private void collectTransformErrorMetrics(TransformErrorMetrics transformErrorMetrics) {
        String metricKey = constructMetricKey(transformErrorMetrics);
        Map<String, Map<String, Long>> domainTransformErrorMap
                = transformErrorMetrics.getTagErrorCntMap();
        for (Entry<String, Map<String, Long>> domainTransformErrorEntry :
                domainTransformErrorMap.entrySet()) {
            String domain = domainTransformErrorEntry.getKey();
            String[] domainList = domain.split(Constants.DOMAIN_DEL);
            Map<String, Long> transformErrorMap = domainTransformErrorEntry.getValue();
            for (Entry<String, Long> transformErrorEntry : transformErrorMap.entrySet()) {
                String tagName = transformErrorEntry.getKey();
                Long tagErrorCnt = transformErrorEntry.getValue();
                StringBuilder sb = new StringBuilder(Constants.SOJ_METRICS_GROUP)
                        .append(metricKey).append(domain)
                        .append(transformErrorMetrics.getEventTime()).append(tagName);
                if (!domainMetricValueMap.containsKey(sb.toString())) {
                    domainMetricValueMap.put(sb.toString(), tagErrorCnt);
                    Gauge gauge = getRuntimeContext()
                            .getMetricGroup().addGroup(Constants.SOJ_METRICS_GROUP)
                            .addGroup(Constants.SOJ_METRIC_TYPE,
                                    transformErrorMetrics.getMetricType().toString())
                            .addGroup(Constants.SOJ_METRIC_NAME,
                                    transformErrorMetrics.getMetricName())
                            .addGroup(Constants.SOJ_PGAE_FAMILY, domainList[0])
                            .addGroup(Constants.SOJ_SITE_ID, domainList[1])
                            .addGroup(Constants.SOJ_EVENT_TIME,
                                    String.valueOf(transformErrorMetrics.getEventTime()))
                            .addGroup(Constants.SOJ_TAG_NAME, String.valueOf(tagName
                                    .replace("|", "-")))
                            .gauge(Constants.TRANSFORM_ERROR_METRICS,
                                    new SojMetricsGauge(domainMetricValueMap,
                                            sb.toString()));
                    domainMetricGuageMap.put(sb.toString(), gauge);
                } else {
                    domainMetricValueMap.put(sb.toString(), tagErrorCnt);
                }
            }

        }
    }

    private String constructMetricKey(TdqMetrics tdqMetrics) {
        MetricType metricType = tdqMetrics.getMetricType();
        StringBuilder metricDesc = new StringBuilder();
        if (metricType != null) {
            metricDesc.append(metricType.name());
        }
        metricDesc.append(Constants.METRIC_DEL).append(tdqMetrics.getMetricName());
        return metricDesc.toString();
    }


}
