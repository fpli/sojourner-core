package com.ebay.sojourner.distributor.function;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import com.ebay.sojourner.common.constant.SojHeaders;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.util.Constants;
import java.util.Date;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DistPipelineMetricsCollectorProcessFunction extends
    ProcessFunction<RawSojEventWrapper, RawSojEventWrapper> {

  private static final String sourceToSink = "source_to_sink";
  private static final String distributorKafkaFetchDelay = "distributor_kafka_fetch_delay";
  private static final String sojournerCustomLatency = "sojourner_custom_e2e_latency";
  private final int latencyWindowSize;
  private transient DropwizardHistogramWrapper sourceToSinkWrapper;
  private transient DropwizardHistogramWrapper distributorKafkaFetchDelayWrapper;
  private transient DropwizardHistogramWrapper sojournerCustomLatencyWrapper;

  public DistPipelineMetricsCollectorProcessFunction(int latencyWindowSize) {
    this.latencyWindowSize = latencyWindowSize;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    sourceToSinkWrapper = getRuntimeContext().getMetricGroup()
        .addGroup(Constants.SOJ_METRICS_GROUP)
        .histogram(sourceToSink, new DropwizardHistogramWrapper(new Histogram(
            new SlidingWindowReservoir(latencyWindowSize))));
    distributorKafkaFetchDelayWrapper = getRuntimeContext().getMetricGroup()
        .addGroup(Constants.SOJ_METRICS_GROUP)
        .histogram(distributorKafkaFetchDelay, new DropwizardHistogramWrapper(new Histogram(
            new SlidingWindowReservoir(latencyWindowSize))));
    sojournerCustomLatencyWrapper = getRuntimeContext().getMetricGroup()
        .addGroup(Constants.SOJ_METRICS_GROUP)
        .histogram(sojournerCustomLatency, new DropwizardHistogramWrapper(new Histogram(
            new SlidingWindowReservoir(latencyWindowSize))));
  }

  @Override
  public void processElement(RawSojEventWrapper value, Context ctx,
      Collector<RawSojEventWrapper> out) {
    long end = new Date().getTime();
    Map<String, Long> timestamps = value.getTimestamps();
    if (timestamps != null) {
      if (timestamps.get(SojHeaders.DISTRIBUTOR_INGEST_TIMESTAMP) != null) {
        long sourceToSink = (end - timestamps.get(SojHeaders.DISTRIBUTOR_INGEST_TIMESTAMP));
        sourceToSinkWrapper.update(sourceToSink);

        if (timestamps.get(SojHeaders.REALTIME_PRODUCER_TIMESTAMP) != null) {
          long distributorKafkaFetchDelayMs =
              timestamps.get(SojHeaders.DISTRIBUTOR_INGEST_TIMESTAMP)
                  - timestamps.get(SojHeaders.REALTIME_PRODUCER_TIMESTAMP);
          distributorKafkaFetchDelayWrapper.update(distributorKafkaFetchDelayMs);
        }
      }
    }
    long sojournerCustomLatency = end - value.getEventTimestamp();
    sojournerCustomLatencyWrapper.update(sojournerCustomLatency);
    out.collect(null);
  }

}
