package com.ebay.sojourner.tdq.function;

import com.ebay.sojourner.common.model.RawEventMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;

public class EmptyMetricFilterFunction extends RichFilterFunction<RawEventMetrics> {

    @Override
    public boolean filter(RawEventMetrics rawEventMetrics) throws Exception {
        return (MapUtils.isNotEmpty(rawEventMetrics.getPageCntMetricsMap()) &&
                rawEventMetrics.getPageCntMetricsMap().size() != 0) ||
                (MapUtils.isNotEmpty(rawEventMetrics.getTagMissingCntMetricsMap()) &&
                        rawEventMetrics.getTagMissingCntMetricsMap().size() != 0) ||
                (MapUtils.isNotEmpty(rawEventMetrics.getTransformErrorMetricsMap()) &&
                        rawEventMetrics.getTransformErrorMetricsMap().size() != 0) ||
                (MapUtils.isNotEmpty(rawEventMetrics.getTagSumMetricsMap()) &&
                        rawEventMetrics.getTagSumMetricsMap().size() != 0);
    }
}