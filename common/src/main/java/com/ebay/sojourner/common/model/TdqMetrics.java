package com.ebay.sojourner.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public abstract class TdqMetrics {
    private MetricType metricType;
    private String metricName;
    private Long eventTime;
    private int taskIndex;
}
