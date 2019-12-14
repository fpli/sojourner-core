package com.ebay.sojourner.ubd.rt.common.broadcast;

import com.ebay.sojourner.ubd.common.model.AgentSignature;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.rt.common.state.MapStateDesc;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class AgentBroadcastProcessFunction extends BroadcastProcessFunction<UbiEvent, AgentSignature,UbiEvent> {

    private static volatile Logger logger = Logger.getLogger(AgentBroadcastProcessFunction.class);

    boolean isSuspectedAgent=false;
    boolean isDeclarativeAgent=false;

    @Override
    public void processElement(UbiEvent ubiEvent, ReadOnlyContext context, Collector<UbiEvent> out) throws Exception {
        ReadOnlyBroadcastState<String, Set<Integer>> agentBroadcastState = context.getBroadcastState(MapStateDesc.agentSignatureDesc);
        if (agentBroadcastState.contains(ubiEvent.getAgentInfo())) {

            if(agentBroadcastState.get(ubiEvent.getAgentInfo())!=null&&agentBroadcastState.get(ubiEvent.getAgentInfo()).size()>0){
                if(agentBroadcastState.get(ubiEvent.getAgentInfo()).contains(220))
                {
                    isSuspectedAgent=true;
                    agentBroadcastState.get(ubiEvent.getAgentInfo()).remove(220);
                }
                if(agentBroadcastState.get(ubiEvent.getAgentInfo()).contains(221))
                {
                    isDeclarativeAgent=true;
                    agentBroadcastState.get(ubiEvent.getAgentInfo()).remove(221);
                }
            }
            ubiEvent.getBotFlags().addAll(agentBroadcastState.get(ubiEvent.getAgentInfo()));
        }

        out.collect(ubiEvent);
    }

    @Override
    public void processBroadcastElement(AgentSignature agentSignature, Context context, Collector<UbiEvent> out) throws Exception {
        BroadcastState<String, Set<Integer>> agentBroadcastState = context.getBroadcastState(MapStateDesc.agentSignatureDesc);
        for (Map.Entry<String, Set<Integer>> entry : agentSignature.getAgentBotSignature().entrySet()) {
            agentBroadcastState.put(entry.getKey(), entry.getValue());
        }
    }
}