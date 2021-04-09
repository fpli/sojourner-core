package com.ebay.sojourner.flink.function;

import com.ebay.sojourner.common.model.SojWatermark;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ExtractWatermarkProcessFunction<T> extends ProcessFunction<T, SojWatermark> {

  private AtomicInteger atomicInteger;
  private transient Long watermarkDelayTime;
  private String metricName;
  private int subtaskIndex;

  public ExtractWatermarkProcessFunction(String metricName) {
    this.metricName = metricName;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    atomicInteger = new AtomicInteger(0);
    getRuntimeContext().getMetricGroup()
        .gauge(metricName, () -> watermarkDelayTime);
    subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
  }

  @Override
  public void processElement(T value, Context ctx, Collector<SojWatermark> out)
      throws Exception {

    watermarkDelayTime = System.currentTimeMillis() - ctx.timestamp();
    int andIncrement = atomicInteger.getAndIncrement();
    if (andIncrement % 1000 == 0) {
      out.collect(new SojWatermark(ctx.timestamp(),subtaskIndex));
    }
  }
}
