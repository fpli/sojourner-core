package com.ebay.sojourner.tdq.function;

import com.ebay.sojourner.common.model.*;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Map.Entry;

public class SojMetricsSplitProcessWindowFunction extends
        ProcessWindowFunction<SojMetrics, SojMetrics, Tuple, TimeWindow> {
    private static final Map<String, Gauge> domainMetricGuageMap = new ConcurrentHashMap<>();
    private static final Map<String, Object> domainMetricValueMap = new ConcurrentHashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<SojMetrics> elements,
                        Collector<SojMetrics> out) {
        SojMetrics sojMetrics = elements.iterator().next();
        Long eventTime = sojMetrics.getEventTime();
        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        sojMetrics.setTaskIndex(taskIndex);
        if (MapUtils.isNotEmpty(sojMetrics.getTagMissingCntMetricsMap())) {
            for (Entry<String, TagMissingCntMetrics> tagMissingCntMetricsEntry :
                    sojMetrics.getTagMissingCntMetricsMap().entrySet()) {
                TagMissingCntMetrics tagMissingCntMetrics
                        = tagMissingCntMetricsEntry.getValue();
                tagMissingCntMetrics.setEventTime(eventTime);
                tagMissingCntMetrics.setTaskIndex(taskIndex);
                context.output(OutputTagConstants.TAG_MISSING_CNT_METRICS_OUTPUT_TAG,
                        tagMissingCntMetrics);
            }
        }
        if (MapUtils.isNotEmpty(sojMetrics.getTagSumMetricsMap())) {
            for (Entry<String, TagSumMetrics> tagSumMetricsEntry :
                    sojMetrics.getTagSumMetricsMap().entrySet()) {
                TagSumMetrics tagSumMetrics = tagSumMetricsEntry.getValue();
                tagSumMetrics.setEventTime(eventTime);
                tagSumMetrics.setTaskIndex(taskIndex);
                context.output(OutputTagConstants.TAG_SUM_METRICS_OUTPUT_TAG, tagSumMetrics);

            }
        }
        if (MapUtils.isNotEmpty(sojMetrics.getPageCntMetricsMap())) {
            for (Entry<String, PageCntMetrics> pageCntMetricsEntry :
                    sojMetrics.getPageCntMetricsMap().entrySet()) {
                PageCntMetrics pageCntMetrics = pageCntMetricsEntry.getValue();
                pageCntMetrics.setEventTime(eventTime);
                pageCntMetrics.setTaskIndex(taskIndex);
                context.output(OutputTagConstants.PAGE_CNT_METRICS_OUTPUT_TAG,
                        pageCntMetrics);

            }
        }
        if (MapUtils.isNotEmpty(sojMetrics.getTransformErrorMetricsMap())) {
            for (Entry<String, TransformErrorMetrics> transformErrorMetricsEntry :
                    sojMetrics.getTransformErrorMetricsMap().entrySet()) {
                TransformErrorMetrics transformErrorMetrics
                        = transformErrorMetricsEntry.getValue();
                transformErrorMetrics.setEventTime(eventTime);
                transformErrorMetrics.setTaskIndex(taskIndex);
                context.output(OutputTagConstants.TRANSFORM_ERROR_METRICS_OUTPUT_TAG,
                        transformErrorMetrics);

            }
        }

        if (MapUtils.isNotEmpty(sojMetrics.getTotalCntMetricsMap())) {
            for (Entry<String, TotalCntMetrics> totalCntMetricsEntry :
                    sojMetrics.getTotalCntMetricsMap().entrySet()) {
                TotalCntMetrics totalCntMetrics
                        = totalCntMetricsEntry.getValue();
                totalCntMetrics.setEventTime(eventTime);
                totalCntMetrics.setTaskIndex(taskIndex);
                context.output(OutputTagConstants.TOTAL_CNT_METRICS_OUTPUT_TAG,
                        totalCntMetrics);

            }
        }
    }
}
