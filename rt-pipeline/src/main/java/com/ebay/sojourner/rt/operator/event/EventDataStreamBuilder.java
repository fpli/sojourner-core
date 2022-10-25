package com.ebay.sojourner.rt.operator.event;

import static com.ebay.sojourner.common.util.Property.SOURCE_EVENT_SLOT_SHARE_GROUP;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.common.DataCenter;
import org.apache.flink.streaming.api.datastream.DataStream;

public class EventDataStreamBuilder {

  public static DataStream<UbiEvent> build(DataStream<RawEvent> sourceDataStream, DataCenter dc) {

    return sourceDataStream
        .flatMap(new LargeMessageHandler(
            FlinkEnvUtils.getLong(Property.MAX_MESSAGE_BYTES),
            FlinkEnvUtils.getInteger(Property.SUB_URL_QUERY_STRING_LENGTH),
            FlinkEnvUtils.getBoolean(Property.TRUNCATE_URL_QUERY_STRING),
            FlinkEnvUtils.getBoolean(Property.DEBUG_MODE)))
        .setParallelism(FlinkEnvUtils.getInteger(Property.EVENT_PARALLELISM))
        .setMaxParallelism(FlinkEnvUtils.getInteger(Property.EVENT_MAX_PARALLELISM))
        .slotSharingGroup(getSlotGroupForDC(dc))
        .name(String.format("Large Message Filter %s", dc))
        .uid(String.format("large-message-filter-%s", dc))
        .map(new EventMapFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.EVENT_PARALLELISM))
        .setMaxParallelism(FlinkEnvUtils.getInteger(Property.EVENT_MAX_PARALLELISM))
        .slotSharingGroup(getSlotGroupForDC(dc))
        .name(String.format("Event Operator %s", dc))
        .uid(String.format("event-operator-%s", dc));
  }

  private static String getSlotGroupForDC(DataCenter dc) {
    String propKey = SOURCE_EVENT_SLOT_SHARE_GROUP + "-" + dc.getValue().toLowerCase();
    return FlinkEnvUtils.getString(propKey);
  }
}
