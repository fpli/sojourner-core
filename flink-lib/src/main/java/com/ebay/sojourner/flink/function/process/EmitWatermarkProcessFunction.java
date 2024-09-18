package com.ebay.sojourner.flink.function.process;

import com.ebay.sojourner.common.model.SojWatermark;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Slf4j
public class EmitWatermarkProcessFunction<T> extends ProcessFunction<T, SojWatermark> {

    private static final Duration duration = Duration.of(1L, ChronoUnit.MINUTES);
    private int subtaskIndex;
    private final String metricName;
    private transient Long watermarkDelayTime;

    private long cacheCurrentTimeMillis;

    public EmitWatermarkProcessFunction(String metricName) {
        this.metricName = metricName;
    }

    @Override
    public void processElement(T value, Context ctx, Collector<SojWatermark> out) {
        watermarkDelayTime = System.currentTimeMillis() - ctx.timestamp();
        if ((System.currentTimeMillis() - cacheCurrentTimeMillis) > duration.toMillis()){
            out.collect(new SojWatermark(ctx.timestamp(), subtaskIndex));
            cacheCurrentTimeMillis = System.currentTimeMillis();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        getRuntimeContext().getMetricGroup().gauge(metricName, () -> watermarkDelayTime);
        cacheCurrentTimeMillis = System.currentTimeMillis();
        log.info("subtaskIndex:{}, cacheCurrentTimeMillis:{}", subtaskIndex, cacheCurrentTimeMillis);
    }
}
