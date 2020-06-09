package com.ebay.sojourner.rt.common.metrics;

import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.rt.common.util.SignatureUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class GuidMetricsCollectorProcessFunction extends
    ProcessFunction<BotSignature, BotSignature> {

  private List<String> signatureIdList;
  private Map<String, Counter> guidCounterNameMap = new ConcurrentHashMap<>();

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    signatureIdList = Arrays.asList("guid_g", "guid_e");

    for (String signatureId : signatureIdList) {
      Counter guidCounter =
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(Constants.SOJ_METRICS_GROUP)
              .counter(signatureId);
      guidCounterNameMap.put(signatureId, guidCounter);
    }
  }

  @Override
  public void processElement(BotSignature value, Context ctx, Collector<BotSignature> out) {

    SignatureUtils
        .signatureMetricsCollection(guidCounterNameMap, value.getType(), value.getIsGeneration());
    out.collect(null);

  }
}