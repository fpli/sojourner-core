package com.ebay.sojourner.tdq.function;

import com.ebay.sojourner.common.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;

import static java.util.Map.Entry;

@Slf4j
public class SojMetricsPostAgg<T extends TdqMetrics> implements AggregateFunction<T, T, T> {
    private Class<T> clz;

    @Override
    public T createAccumulator() {
        T accMetrics = null;
        try {
            accMetrics = clz.newInstance();
        } catch (Exception e) {
            log.error("initial accMetrics error");
        }
        return accMetrics;
    }

    public SojMetricsPostAgg(Class<T> clz) {
        this.clz = clz;
    }

    @Override
    public T add(T inputMetrics,
                 T accMetrics) {
        if (inputMetrics instanceof TagMissingCntMetrics) {
            addTagMissingCntMetrics((TagMissingCntMetrics) inputMetrics,
                    (TagMissingCntMetrics) accMetrics);
        } else if (inputMetrics instanceof TagSumMetrics) {
            addTagSumMetrics((TagSumMetrics) inputMetrics, (TagSumMetrics) accMetrics);
        } else if (inputMetrics instanceof PageCntMetrics) {
            addPageCntMetrics((PageCntMetrics) inputMetrics, (PageCntMetrics) accMetrics);
        } else if (inputMetrics instanceof TransformErrorMetrics) {
            addTransformErrorCntMetrics((TransformErrorMetrics) inputMetrics,
                    (TransformErrorMetrics) accMetrics);
        } else if (inputMetrics instanceof TotalCntMetrics) {
            addTotalCntMetrics((TotalCntMetrics) inputMetrics, (TotalCntMetrics) accMetrics);
        }
        return accMetrics;
    }

    @Override
    public T getResult(T accMetrics) {
        return accMetrics;
    }

    @Override
    public T merge(T a, T b) {
        return null;
    }

    private void addTagMissingCntMetrics(TagMissingCntMetrics tagMissingCntMetrics,
                                         TagMissingCntMetrics accTagMissingCntMetrics) {
        accTagMissingCntMetrics.setMetricType(tagMissingCntMetrics.getMetricType());
        accTagMissingCntMetrics.setMetricName(tagMissingCntMetrics.getMetricName());
        accTagMissingCntMetrics.getPageFamilySet()
                .addAll(tagMissingCntMetrics.getPageFamilySet());
        accTagMissingCntMetrics.setEventTime(tagMissingCntMetrics.getEventTime());
        Map<String, Map<String, Long>> domainTagCntMap = tagMissingCntMetrics.getTagCntMap();
        Map<String, Map<String, Long>> sojDomainTagCntMap = accTagMissingCntMetrics.getTagCntMap();
        for (Entry<String, Map<String, Long>> domainTagCntEntry : domainTagCntMap.entrySet()) {
            String domain = domainTagCntEntry.getKey();
            if (sojDomainTagCntMap.containsKey(domain)) {
                Map<String, Long> tagMissingCntMap = domainTagCntEntry.getValue();
                Map<String, Long> sojTagCntMap = sojDomainTagCntMap.get(domain);
                for (Entry<String, Long> tagCntEntry : tagMissingCntMap.entrySet()) {
                    String tagName = tagCntEntry.getKey();
                    Long count = tagCntEntry.getValue();
                    if (sojTagCntMap.containsKey(tagName)) {
                        sojTagCntMap.put(tagName, sojTagCntMap.get(tagName) + count);
                    } else {
                        sojTagCntMap.put(tagName, count);
                    }
                }
            } else {
                sojDomainTagCntMap.put(domain, domainTagCntEntry.getValue());
            }
        }
    }

    private void addTagSumMetrics(TagSumMetrics tagSumMetrics, TagSumMetrics accTagSumMetrics) {
        Map<String, Map<String, Double>> domainTagSumMap = tagSumMetrics.getTagSumMap();
        Map<String, Map<String, Double>> sojDomainTagSumMap = accTagSumMetrics.getTagSumMap();
        accTagSumMetrics.setMetricType(tagSumMetrics.getMetricType());
        accTagSumMetrics.setMetricName(tagSumMetrics.getMetricName());
        accTagSumMetrics.setEventTime(tagSumMetrics.getEventTime());
        for (Entry<String, Map<String, Double>> tagCntEntry : domainTagSumMap.entrySet()) {
            String domain = tagCntEntry.getKey();
            if (sojDomainTagSumMap.containsKey(domain)) {
                Map<String, Double> tagSum = tagCntEntry.getValue();
                Map<String, Double> sojTagSumMap = sojDomainTagSumMap.get(domain);
                for (Entry<String, Double> tagSumEntry : tagSum.entrySet()) {
                    String tagName = tagSumEntry.getKey();
                    Double tagValue = tagSumEntry.getValue();
                    if (sojTagSumMap.containsKey(tagName)) {
                        sojTagSumMap.put(tagName, sojTagSumMap.get(tagName) + tagValue);
                    } else {
                        sojTagSumMap.put(tagName, tagValue);
                    }
                }
            } else {
                sojDomainTagSumMap.put(domain, tagCntEntry.getValue());
            }
        }
    }

    private void addPageCntMetrics(PageCntMetrics pageCntMetrics,
                                   PageCntMetrics accPageCntMetrics) {
        Map<String, Map<Integer, Long>> sojDomainPageCntMap = accPageCntMetrics.getPageCntMap();
        Map<String, Map<Integer, Long>> domainPageCntMap = pageCntMetrics.getPageCntMap();
        accPageCntMetrics.setMetricType(pageCntMetrics.getMetricType());
        accPageCntMetrics.setMetricName(pageCntMetrics.getMetricName());
        accPageCntMetrics.setEventTime(pageCntMetrics.getEventTime());
        for (Entry<String, Map<Integer, Long>> domainPageCntEntry
                : domainPageCntMap.entrySet()) {
            String domain = domainPageCntEntry.getKey();
            if (sojDomainPageCntMap.containsKey(domain)) {
                Map<Integer, Long> pageCntMap = domainPageCntEntry.getValue();
                Map<Integer, Long> sojPageCntMap = sojDomainPageCntMap.get(domain);
                for (Entry<Integer, Long> pageCntEntry : pageCntMap.entrySet()) {
                    Integer pageId = pageCntEntry.getKey();
                    Long count = pageCntEntry.getValue();
                    if (sojPageCntMap.containsKey(pageId)) {
                        sojPageCntMap.put(pageId, sojPageCntMap.get(pageId) + count);
                    } else {
                        sojPageCntMap.put(pageId, count);
                    }
                }
            } else {
                sojDomainPageCntMap.put(domain, domainPageCntEntry.getValue());
            }
        }
    }

    private void addTotalCntMetrics(TotalCntMetrics totalCntMetrics,
                                    TotalCntMetrics accTotalCntMetrics) {
        Map<String, Long> domainTotalCntMap = totalCntMetrics.getTotalCntMap();
        Map<String, Long> sojDomainTotalCntMap = accTotalCntMetrics.getTotalCntMap();
        accTotalCntMetrics.setMetricType(totalCntMetrics.getMetricType());
        accTotalCntMetrics.setMetricName(totalCntMetrics.getMetricName());
        accTotalCntMetrics.setEventTime(totalCntMetrics.getEventTime());
        for (Entry<String, Long> domainTotalCntEntry
                : domainTotalCntMap.entrySet()) {
            String domain = domainTotalCntEntry.getKey();
            if (sojDomainTotalCntMap.containsKey(domain)) {
                Long totalCnt = domainTotalCntEntry.getValue();
                Long sojTotalCnt = sojDomainTotalCntMap.get(domain);
                sojDomainTotalCntMap.put(domain, totalCnt + sojTotalCnt);
            } else {
                sojDomainTotalCntMap.put(domain, domainTotalCntEntry.getValue());
            }
        }
        Map<String, Long> domainTotalCntItmMap = totalCntMetrics.getTotalCntItmMap();
        Map<String, Long> sojDomainTotalCntItmMap = accTotalCntMetrics.getTotalCntItmMap();
        for (Entry<String, Long> domainTotalCntItmEntry
                : domainTotalCntItmMap.entrySet()) {
            String domain = domainTotalCntItmEntry.getKey();
            if (sojDomainTotalCntItmMap.containsKey(domain)) {
                Long totalCnt = domainTotalCntItmEntry.getValue();
                Long sojTotalCnt = sojDomainTotalCntItmMap.get(domain);
                sojDomainTotalCntItmMap.put(domain, totalCnt + sojTotalCnt);
            } else {
                sojDomainTotalCntItmMap.put(domain, domainTotalCntItmEntry.getValue());
            }
        }
    }

    private void addTransformErrorCntMetrics(TransformErrorMetrics transformErrorMetrics,
                                             TransformErrorMetrics accTransformErrorMetrics) {
        Map<String, Map<String, Long>> domainTagErrorCntMap
                = transformErrorMetrics.getTagErrorCntMap();
        Map<String, Map<String, Long>> sojDomainTagErrorCntMap
                = accTransformErrorMetrics.getTagErrorCntMap();
        accTransformErrorMetrics.setMetricType(transformErrorMetrics.getMetricType());
        accTransformErrorMetrics.setMetricName(transformErrorMetrics.getMetricName());
        accTransformErrorMetrics.setEventTime(transformErrorMetrics.getEventTime());

        for (Entry<String, Map<String, Long>> domainTagErrorCntEntry
                : domainTagErrorCntMap.entrySet()) {
            String domain = domainTagErrorCntEntry.getKey();
            if (sojDomainTagErrorCntMap.containsKey(domain)) {
                Map<String, Long> tagErrorCntMap = domainTagErrorCntEntry.getValue();
                Map<String, Long> sojTagErrorCntMap = sojDomainTagErrorCntMap.get(domain);
                for (Entry<String, Long> tagCntEntry : tagErrorCntMap.entrySet()) {
                    String tagName = tagCntEntry.getKey();
                    Long count = tagCntEntry.getValue();
                    if (sojTagErrorCntMap.containsKey(tagName)) {
                        sojTagErrorCntMap.put(tagName,
                                sojTagErrorCntMap.get(tagName) + count);
                    } else {
                        sojTagErrorCntMap.put(tagName, count);
                    }
                }
            } else {
                sojDomainTagErrorCntMap.put(domain, domainTagErrorCntEntry.getValue());
            }
        }
    }
}


