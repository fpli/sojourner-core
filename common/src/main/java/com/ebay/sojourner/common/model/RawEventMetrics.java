package com.ebay.sojourner.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class RawEventMetrics implements Serializable {
    private String guid;
    private Map<String,TagMissingCntMetrics> tagMissingCntMetricsMap = new HashMap<>();
    private Map<String,TagSumMetrics> tagSumMetricsMap = new HashMap<>();
    private Map<String,PageCntMetrics> pageCntMetricsMap = new HashMap<>();
    private Map<String,TransformErrorMetrics> transformErrorMetricsMap = new HashMap<>();
    private Map<String,TotalCntMetrics> totalCntMetricsMap = new HashMap<>();
}