package com.ebay.sojourner.rt.metric;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.SojTimestamp;
import java.util.Date;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PipelineMetricsCollectorProcessFunction extends ProcessFunction<UbiEvent, UbiEvent> {

  private static final String siteToSource = "site_to_source";
  private static final String siteToSink = "site_to_sink";
  private static final String sourceToSink = "source_to_sink";
  private final int latencyWindowSize;
  private transient DropwizardHistogramWrapper siteToSourceWrapper;
  private transient DropwizardHistogramWrapper siteToSinkWrapper;
  private transient DropwizardHistogramWrapper sourceToSinkWrapper;

  public PipelineMetricsCollectorProcessFunction(int latencyWindowSize) {
    this.latencyWindowSize = latencyWindowSize;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    siteToSourceWrapper = getRuntimeContext().getMetricGroup()
        .addGroup(Constants.SOJ_METRICS_GROUP)
        .histogram(siteToSource, new DropwizardHistogramWrapper(new Histogram(
            new SlidingWindowReservoir(latencyWindowSize))));
    siteToSinkWrapper = getRuntimeContext().getMetricGroup()
        .addGroup(Constants.SOJ_METRICS_GROUP)
        .histogram(siteToSink, new DropwizardHistogramWrapper(new Histogram(
            new SlidingWindowReservoir(latencyWindowSize))));
    sourceToSinkWrapper = getRuntimeContext().getMetricGroup()
        .addGroup(Constants.SOJ_METRICS_GROUP)
        .histogram(sourceToSink, new DropwizardHistogramWrapper(new Histogram(
            new SlidingWindowReservoir(latencyWindowSize))));
  }

  @Override
  public void processElement(UbiEvent value, Context ctx, Collector<UbiEvent> out) {
    long end = new Date().getTime();
    long siteToSource =
        value.getIngestTime() - SojTimestamp
            .getSojTimestampToUnixTimestamp(value.getEventTimestamp());
    long siteToSink = end - SojTimestamp
        .getSojTimestampToUnixTimestamp(value.getEventTimestamp());
    long sourceToSink = (end - value.getIngestTime());
    siteToSourceWrapper.update(siteToSource);
    siteToSinkWrapper.update(siteToSink);
    sourceToSinkWrapper.update(sourceToSink);
    out.collect(null);
  }
}
