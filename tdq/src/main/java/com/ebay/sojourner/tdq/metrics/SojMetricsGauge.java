package com.ebay.sojourner.tdq.metrics;

import org.apache.flink.metrics.Gauge;

import java.util.Map;

public class SojMetricsGauge implements Gauge<Object> {
    private Map<String, Object> sojMetricsMap;
    private String metricKey;

    public SojMetricsGauge(Map<String, Object> sojMetricsMap, String metricKey) {
        this.sojMetricsMap = sojMetricsMap;
        this.metricKey = metricKey;
    }

    public Object getValue() {
        return sojMetricsMap.get(metricKey);
    }
}