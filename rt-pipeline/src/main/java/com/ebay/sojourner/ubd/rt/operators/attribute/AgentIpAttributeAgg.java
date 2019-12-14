package com.ebay.sojourner.ubd.rt.operators.attribute;

import com.ebay.sojourner.ubd.common.model.AgentIpAttributeAccumulator;
import com.ebay.sojourner.ubd.common.model.UbiSession;
import com.ebay.sojourner.ubd.common.sharedlib.indicators.AgentIpIndicators;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.log4j.Logger;

public class AgentIpAttributeAgg implements AggregateFunction<UbiSession, AgentIpAttributeAccumulator, AgentIpAttributeAccumulator> {
    private static final Logger logger = Logger.getLogger(AgentIpAttributeAgg.class);
    private AgentIpIndicators agentIpIndicators;

    @Override
    public AgentIpAttributeAccumulator createAccumulator() {

        AgentIpAttributeAccumulator agentIpAttributeAccumulator = new AgentIpAttributeAccumulator();
        agentIpIndicators = AgentIpIndicators.getInstance();

        try {
            agentIpIndicators.start(agentIpAttributeAccumulator);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return agentIpAttributeAccumulator;
    }

    @Override
    public AgentIpAttributeAccumulator add(UbiSession session, AgentIpAttributeAccumulator agentIpAttributeAccumulator) {
        if (agentIpAttributeAccumulator.getAgentIpAttribute().getClientIp() == null&&agentIpAttributeAccumulator.getAgentIpAttribute().getAgent()==null) {
            agentIpAttributeAccumulator.getAgentIpAttribute().setClientIp(session.getClientIp());
            agentIpAttributeAccumulator.getAgentIpAttribute().setAgent(session.getUserAgent());
        }
        try {
            agentIpIndicators.feed(session,agentIpAttributeAccumulator);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return agentIpAttributeAccumulator;
    }

    @Override
    public AgentIpAttributeAccumulator getResult(AgentIpAttributeAccumulator agentIpAttributeAccumulator) {
        return agentIpAttributeAccumulator;
    }

    @Override
    public AgentIpAttributeAccumulator merge(AgentIpAttributeAccumulator a, AgentIpAttributeAccumulator b) {
        return null;
    }
}