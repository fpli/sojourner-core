package com.ebay.sojourner.tdq.function;

import com.ebay.sojourner.common.model.SojMetrics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SojMetricsProcessWindowFunction extends
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
        Long eventTime = context.window().getEnd();
        //        collectSojMetrics(sojMetrics, context.window().getEnd());
        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        sojMetrics.setTaskIndex(taskIndex);
        sojMetrics.setEventTime(eventTime);
        out.collect(sojMetrics);
    }
}
