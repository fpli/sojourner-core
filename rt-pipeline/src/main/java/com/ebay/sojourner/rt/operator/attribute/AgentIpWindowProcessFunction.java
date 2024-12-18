package com.ebay.sojourner.rt.operator.attribute;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.AgentIpAttributeAccumulator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AgentIpWindowProcessFunction extends
    ProcessWindowFunction<AgentIpAttributeAccumulator, AgentIpAttribute, Tuple, TimeWindow> {

  @Override
  public void process(
      Tuple tuple,
      Context context,
      Iterable<AgentIpAttributeAccumulator> elements,
      Collector<AgentIpAttribute> out) {

    AgentIpAttributeAccumulator agentIpAttributeAccumulator = elements.iterator().next();
    out.collect(agentIpAttributeAccumulator.getAgentIpAttribute());
  }
}
