package com.ebay.sojourner.rt.pipeline;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventDeserializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventKafkaDeserializationSchemaWrapper;
import com.ebay.sojourner.rt.operator.event.EventDataStreamBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN;
import static com.ebay.sojourner.flink.common.DataCenter.SLC;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

public class SojournerRTJobForQA extends SojournerRTJob {

  public static void main(String[] args) throws Exception {
    new SojournerRTJobForQA().run(args);
  }

  protected DataStream<UbiEvent> generateEventDataStream(
          StreamExecutionEnvironment executionEnvironment) {

    // 1. Rheos Consumer
    // 1.1 Consume RawEvent from Rheos PathFinder topic
    // 1.2 Assign timestamps and emit watermarks.
    SourceDataStreamBuilder<RawEvent> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<RawEvent> rawEventDataStreamForSLC = dataStreamBuilder
        .dc(SLC)
        .operatorName(getString(Property.SOURCE_OPERATOR_NAME_SLC))
        .uid(getString(Property.SOURCE_UID_SLC))
        .slotGroup(getString(Property.SOURCE_EVENT_SLC_SLOT_SHARE_GROUP))
        .outOfOrderlessInMin(getInteger(FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN))
        .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
        .idleSourceTimeout(getInteger(Property.FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN))
        .build(new RawEventKafkaDeserializationSchemaWrapper(
            FlinkEnvUtils.getSet(Property.FILTER_GUID_SET),
            new RawEventDeserializationSchema(
                FlinkEnvUtils.getString(Property.RHEOS_KAFKA_REGISTRY_URL))));

    // 2. Event Operator
    // 2.1 Parse and transform RawEvent to UbiEvent
    // 2.2 Event level bot detection via bot rule
    DataStream<UbiEvent> ubiEventDataStreamForSLC = EventDataStreamBuilder
        .build(rawEventDataStreamForSLC, SLC);

    // union ubiEvent from SLC/RNO/LVS (Staging has SLC only)
    DataStream<UbiEvent> ubiEventDataStream = ubiEventDataStreamForSLC;
    return ubiEventDataStream;
  }
}
