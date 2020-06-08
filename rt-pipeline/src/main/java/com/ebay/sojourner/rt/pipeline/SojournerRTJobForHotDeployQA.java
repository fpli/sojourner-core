package com.ebay.sojourner.rt.pipeline;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.rt.common.metrics.EventMetricsCollectorProcessFunction;
import com.ebay.sojourner.flink.connectors.kafka.KafkaSourceFunction;
import com.ebay.sojourner.rt.common.util.Constants;
import com.ebay.sojourner.rt.operators.event.EventMapFunction;
import com.ebay.sojourner.flink.common.env.FlinkEnvUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojournerRTJobForHotDeployQA {

  public static void main(String[] args) throws Exception {

    // 0.0 Prepare execution environment
    // 0.1 UBI configuration
    // 0.2 Flink configuration
    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    // for soj nrt output
    // 1. Rheos Consumer
    // 1.1 Consume RawEvent from Rheos PathFinder topic
    // 1.2 Assign timestamps and emit watermarks.
    DataStream<RawEvent> rawEventDataStream =
        executionEnvironment
            .addSource(KafkaSourceFunction
                .buildSource(FlinkEnvUtils.getString(Constants.BEHAVIOR_PATHFINDER_TOPIC),
                    FlinkEnvUtils
                        .getListString(Constants.BEHAVIOR_PATHFINDER_BOOTSTRAP_SERVERS_LVS),
                    FlinkEnvUtils.getString(Constants.BEHAVIOR_PATHFINDER_GROUP_ID_DEFAULT_LVS),
                    RawEvent.class))
            .setParallelism(
                FlinkEnvUtils.getInteger(Constants.SOURCE_PARALLELISM))
            .name("Rheos Kafka Consumer For QA")
            .uid("source-id");

    // 2. Event Operator
    // 2.1 Parse and transform RawEvent to UbiEvent
    // 2.2 Event level bot detection via bot rule
    DataStream<UbiEvent> ubiEventDataStream =
        rawEventDataStream
            .map(new EventMapFunction())
            .setParallelism(FlinkEnvUtils.getInteger(Constants.EVENT_PARALLELISM))
            .name("Event Operator")
            .uid("event-id");

    // event metrics collector
    ubiEventDataStream
        .process(new EventMetricsCollectorProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Constants.METRICS_PARALLELISM))
        .name("Event Metrics Collector")
        .uid("event-metrics-id");

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, FlinkEnvUtils.getString(Constants.NAME_HOT_DEPLOY));
  }
}
