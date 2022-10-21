package com.ebay.sojourner.rt.pipeline;

import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojournerRTJobForDev extends SojournerRTJobForQA {

  public static void main(String[] args) throws Exception {
    new SojournerRTJobForDev().run(args);
  }

  protected StreamExecutionEnvironment prepareStreamExecutionEnvironment(String[] args) {
    StreamExecutionEnvironment env = FlinkEnvUtils.prepareLocal(args);
    return env;
  }
}
