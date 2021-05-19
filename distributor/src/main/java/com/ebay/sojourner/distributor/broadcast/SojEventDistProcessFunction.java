package com.ebay.sojourner.distributor.broadcast;

import com.ebay.sojourner.common.model.PageIdTopicMapping;
import com.ebay.sojourner.common.model.RawSojEventWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class SojEventDistProcessFunction extends
    BroadcastProcessFunction<RawSojEventWrapper, PageIdTopicMapping, RawSojEventWrapper> {

  private final MapStateDescriptor<Integer, PageIdTopicMapping> stateDescriptor;
  private final int ALL_PAGE = 0;

  public SojEventDistProcessFunction(MapStateDescriptor<Integer, PageIdTopicMapping> descriptor) {
    this.stateDescriptor = descriptor;
  }

  @Override
  public void processElement(RawSojEventWrapper sojEventWrapper, ReadOnlyContext ctx,
                             Collector<RawSojEventWrapper> out) throws Exception {
    ReadOnlyBroadcastState<Integer, PageIdTopicMapping> broadcastState =
        ctx.getBroadcastState(stateDescriptor);

    // distribute events based on pageid
    int pageId = sojEventWrapper.getPageId();
    PageIdTopicMapping mapping = broadcastState.get(pageId);
    if (mapping != null) {
      for (String topic : mapping.getTopics()) {
        sojEventWrapper.setTopic(topic);
        out.collect(sojEventWrapper);
      }
    }

    // distribute all events if all_page is set
    if (broadcastState.get(ALL_PAGE) != null) {
      for (String topic : broadcastState.get(ALL_PAGE).getTopics()) {
        sojEventWrapper.setTopic(topic);
        out.collect(sojEventWrapper);
      }
    }
  }

  @Override
  public void processBroadcastElement(PageIdTopicMapping mapping, Context ctx,
                                      Collector<RawSojEventWrapper> out) throws Exception {
    log.info("process broadcast pageId topic mapping: {}", mapping);
    BroadcastState<Integer, PageIdTopicMapping> broadcastState =
        ctx.getBroadcastState(stateDescriptor);
    if (mapping.getTopics() == null) {
      // remove topic/pageid from state
      broadcastState.remove(mapping.getPageId());
    } else {
      broadcastState.put(mapping.getPageId(), mapping);
    }
  }
}
