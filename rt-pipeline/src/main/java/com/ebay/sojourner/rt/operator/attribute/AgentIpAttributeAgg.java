package com.ebay.sojourner.rt.operator.attribute;

import com.ebay.sojourner.business.indicator.AgentIpIndicators;
import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.AgentIpAttributeAccumulator;
import com.ebay.sojourner.common.model.SessionCore;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

@Slf4j
public class AgentIpAttributeAgg implements
    AggregateFunction<SessionCore, AgentIpAttributeAccumulator, AgentIpAttributeAccumulator> {

  @Override
  public AgentIpAttributeAccumulator createAccumulator() {

    AgentIpAttributeAccumulator agentIpAttributeAccumulator = new AgentIpAttributeAccumulator();

    try {
      AgentIpIndicators.getInstance().start(agentIpAttributeAccumulator);
    } catch (Exception e) {
      log.error("init pre agent ip indicators failed", e);
    }

    return agentIpAttributeAccumulator;
  }

  @Override
  public AgentIpAttributeAccumulator add(SessionCore sessionCore,
      AgentIpAttributeAccumulator agentIpAttributeAccumulator) {

    AgentIpAttribute agentIpAttribute = agentIpAttributeAccumulator.getAgentIpAttribute();

    if (agentIpAttribute.getClientIp() == null && agentIpAttribute.getAgent() == null) {
      agentIpAttribute.setClientIp(sessionCore.getIp());
      agentIpAttribute.setAgent(sessionCore.getUserAgent());
    }

    try {
      AgentIpIndicators.getInstance().feed(sessionCore, agentIpAttributeAccumulator);
    } catch (Exception e) {
      log.error("start pre agent ip indicators collection failed", e);
    }

    return agentIpAttributeAccumulator;
  }

  @Override
  public AgentIpAttributeAccumulator getResult(
      AgentIpAttributeAccumulator agentIpAttributeAccumulator) {
    return agentIpAttributeAccumulator;
  }

  @Override
  public AgentIpAttributeAccumulator merge(
      AgentIpAttributeAccumulator a, AgentIpAttributeAccumulator b) {
    return null;
  }
}
