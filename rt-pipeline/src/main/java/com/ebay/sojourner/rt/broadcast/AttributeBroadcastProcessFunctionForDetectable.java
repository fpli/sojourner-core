package com.ebay.sojourner.rt.broadcast;

import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.SojTimestamp;
import com.ebay.sojourner.common.util.TypeTransformUtil;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.state.MapStateDesc;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class AttributeBroadcastProcessFunctionForDetectable extends
    BroadcastProcessFunction<Either<UbiEvent, UbiSession>, BotSignature, UbiEvent> {

  private OutputTag outputTag;
  private Counter ipCounter;
  private Counter agentCounter;
  private Counter agentIpCounter;
  private Counter ipIncCounter;
  private Counter ipDecCounter;
  private Counter agentIncCounter;
  private Counter agentDecCounter;
  private Counter agentIpIncCounter;
  private Counter agentIpDecCounter;
  private Set<String> agentWhiteList;
  public AttributeBroadcastProcessFunctionForDetectable(OutputTag sessionOutputTag) {
    outputTag = sessionOutputTag;
  }

  @Override
  public void processElement(Either<UbiEvent, UbiSession> signatureDetectable,
      ReadOnlyContext context, Collector<UbiEvent> out) throws Exception {

    ReadOnlyBroadcastState<String, Map<String, Map<Integer, Long[]>>> attributeSignature =
        context.getBroadcastState(MapStateDesc.attributeSignatureDesc);

    if (signatureDetectable.isLeft()) {
      UbiEvent ubiEvent = signatureDetectable.left();

      // ip
      String ip = TypeTransformUtil.ipToInt(ubiEvent.getIp()) == null ? "0"
          : TypeTransformUtil.ipToInt(ubiEvent.getIp()).toString();
      Map<String, Map<Integer, Long[]>> ipSignature = attributeSignature.get("ip");
      if (ipSignature != null && ipSignature.size() > 0 && ipSignature.containsKey(ip)) {
        for (Map.Entry<Integer, Long[]> ipBotFlagMap :
            ipSignature.get(ip).entrySet()) {
          Long[] duration = ipBotFlagMap.getValue();
          if (SojTimestamp.getSojTimestampToUnixTimestamp(ubiEvent.getEventTimestamp())
              > duration[0]
              && SojTimestamp.getSojTimestampToUnixTimestamp(ubiEvent.getEventTimestamp())
              < duration[1]) {
            ubiEvent.getBotFlags().add(ipBotFlagMap.getKey());
          }
        }
      }

      // agent
      long[] long4AgentHash = TypeTransformUtil
          .md522Long(TypeTransformUtil.getMD5(ubiEvent.getUserAgent()));
      Map<String, Map<Integer, Long[]>> agentSignature = attributeSignature.get("agent");
      String agent = long4AgentHash[0] + Constants.FIELD_DELIM + long4AgentHash[1];

      if (agentSignature != null && agentSignature.size() > 0
          && agentSignature.containsKey(agent)&&!agentWhiteList.contains(agent)) {
        for (Map.Entry<Integer, Long[]> agentBotFlagMap :
            agentSignature.get(agent).entrySet()) {
          Long[] duration = agentBotFlagMap.getValue();
          if (SojTimestamp.getSojTimestampToUnixTimestamp(ubiEvent.getEventTimestamp())
              > duration[0]
              && SojTimestamp.getSojTimestampToUnixTimestamp(ubiEvent.getEventTimestamp())
              < duration[1]) {
            ubiEvent.getBotFlags().add(agentBotFlagMap.getKey());
          }
        }
      }

      // agentIp
      String agentIp =
          long4AgentHash[0] + Constants.FIELD_DELIM + long4AgentHash[1] + Constants.FIELD_DELIM + (
              TypeTransformUtil.ipToInt(ubiEvent.getIp()) == null ? "0"
                  : TypeTransformUtil.ipToInt(ubiEvent.getIp()).toString());
      Map<String, Map<Integer, Long[]>> agentIpSignature = attributeSignature.get("agentIp");
      if (agentIpSignature != null && agentIpSignature.size() > 0
          && agentIpSignature.containsKey(agentIp)) {
        for (Map.Entry<Integer, Long[]> agentIpBotFlagMap :
            agentIpSignature.get(agentIp).entrySet()) {
          Long[] duration = agentIpBotFlagMap.getValue();
          if (SojTimestamp.getSojTimestampToUnixTimestamp(ubiEvent.getEventTimestamp())
              > duration[0]
              && SojTimestamp.getSojTimestampToUnixTimestamp(ubiEvent.getEventTimestamp())
              < duration[1]) {
            ubiEvent.getBotFlags().add(agentIpBotFlagMap.getKey());
          }
        }
      }

      //      if ((UbiSessionHelper.isAgentDeclarative(ubiEvent.getUserAgent()) && ubiEvent
      //      .getBotFlags()
      //          .contains(223))
      //          || (ubiEvent.getBotFlags().contains(220) && ubiEvent.getBotFlags().contains
      //          (222))) {
      //        ubiEvent.getBotFlags().add(202);
      //      }
      //
      //      if ((UbiSessionHelper.isAgentDeclarative(ubiEvent.getUserAgent()) && ubiEvent
      //      .getBotFlags()
      //          .contains(223))
      //          || (ubiEvent.getBotFlags().contains(220) && ubiEvent.getBotFlags().contains
      //          (222))) {
      //        ubiEvent.getBotFlags().add(210);
      //      }
      //
      //      if (ubiEvent.getBotFlags().contains(223) && ubiEvent.getBotFlags().contains(224)) {
      //        ubiEvent.getBotFlags().add(211);
      //      }
      out.collect(ubiEvent);

    } else {

      UbiSession ubiSession = signatureDetectable.right();

      // ip
      String ip = TypeTransformUtil.ipToInt(ubiSession.getIp()) == null ? "0"
          : TypeTransformUtil.ipToInt(ubiSession.getIp()).toString();
      Map<String, Map<Integer, Long[]>> ipSignature = attributeSignature.get("ip");
      if (ipSignature != null && ipSignature.size() > 0 && ipSignature.containsKey(ip)) {
        for (Map.Entry<Integer, Long[]> ipBotFlagMap :
            ipSignature.get(ip).entrySet()) {
          Long[] duration = ipBotFlagMap.getValue();
          if (SojTimestamp.getSojTimestampToUnixTimestamp(ubiSession.getAbsStartTimestamp())
              > duration[0]
              && SojTimestamp.getSojTimestampToUnixTimestamp(ubiSession.getAbsStartTimestamp())
              < duration[1]) {
            ubiSession.getBotFlagList().add(ipBotFlagMap.getKey());
          }
        }
      }

      // agent
      long[] long4AgentHash = TypeTransformUtil
          .md522Long(TypeTransformUtil.getMD5(ubiSession.getUserAgent()));
      Map<String, Map<Integer, Long[]>> agentSignature = attributeSignature.get("agent");
      String agent = long4AgentHash[0] + Constants.FIELD_DELIM + long4AgentHash[1];
      if (agentSignature != null && agentSignature.size() > 0
          && agentSignature.containsKey(agent)) {
        for (Map.Entry<Integer, Long[]> agentBotFlagMap :
            agentSignature.get(agent).entrySet()) {
          Long[] duration = agentBotFlagMap.getValue();
          if (SojTimestamp.getSojTimestampToUnixTimestamp(ubiSession.getAbsStartTimestamp())
              > duration[0]
              && SojTimestamp.getSojTimestampToUnixTimestamp(ubiSession.getAbsStartTimestamp())
              < duration[1]) {
            ubiSession.getBotFlagList().add(agentBotFlagMap.getKey());
          }
        }
      }

      // agentIp
      String agentIp =
          long4AgentHash[0] + Constants.FIELD_DELIM + long4AgentHash[1] + Constants.FIELD_DELIM + (
              TypeTransformUtil.ipToInt(ubiSession.getIp()) == null ? "0"
                  : TypeTransformUtil.ipToInt(ubiSession.getIp()).toString());
      Map<String, Map<Integer, Long[]>> agentIpSignature = attributeSignature.get("agentIp");
      if (agentIpSignature != null && agentIpSignature.size() > 0
          && agentIpSignature.containsKey(agentIp)) {
        for (Map.Entry<Integer, Long[]> agentIpBotFlagMap :
            agentIpSignature.get(agentIp).entrySet()) {
          Long[] duration = agentIpBotFlagMap.getValue();
          if (SojTimestamp.getSojTimestampToUnixTimestamp(ubiSession.getAbsStartTimestamp())
              > duration[0]
              && SojTimestamp.getSojTimestampToUnixTimestamp(ubiSession.getAbsStartTimestamp())
              < duration[1]) {
            ubiSession.getBotFlagList().add(agentIpBotFlagMap.getKey());
          }
        }
      }

      //      if ((UbiSessionHelper.isAgentDeclarative(ubiSession.getUserAgent())
      //          && ubiSession.getBotFlagList().contains(223))
      //          || (ubiSession.getBotFlagList().contains(220)
      //          && ubiSession.getBotFlagList().contains(222))) {
      //        ubiSession.getBotFlagList().add(202);
      //      }
      //
      //      if ((UbiSessionHelper.isAgentDeclarative(ubiSession.getUserAgent())
      //          && ubiSession.getBotFlagList().contains(223))
      //          || (ubiSession.getBotFlagList().contains(220)
      //          && ubiSession.getBotFlagList().contains(222))) {
      //        ubiSession.getBotFlagList().add(210);
      //      }
      //
      //      if (ubiSession.getBotFlagList().contains(223) && ubiSession.getBotFlagList()
      //      .contains(224)) {
      //        ubiSession.getBotFlagList().add(211);
      //      }
      context.output(outputTag, ubiSession);

    }
  }

  @Override
  public void processBroadcastElement(BotSignature attributeSignature, Context context,
      Collector<UbiEvent> out) throws Exception {

    BroadcastState<String, Map<String, Map<Integer, Long[]>>> attributeBroadcastStatus =
        context.getBroadcastState(MapStateDesc.attributeSignatureDesc);

    List<Integer> botFlags = attributeSignature.getBotFlags();
    String signatureId = null;
    Long expirationTime = null;
    Boolean isGeneration = null;
    int category = 0;
    Long generationTime = null;
    Map<String, Map<Integer, Long[]>> signature =
        attributeBroadcastStatus.get(attributeSignature.getType());
    expirationTime = attributeSignature.getExpirationTime();
    isGeneration = attributeSignature.getIsGeneration();
    category = attributeSignature.getCategory();
    generationTime = attributeSignature.getGenerationTime();
    if ("agent".equals(attributeSignature.getType())) {
      signatureId =
          attributeSignature.getUserAgent().getAgentHash1() + Constants.FIELD_DELIM
              + attributeSignature
              .getUserAgent().getAgentHash2();

    } else if ("agentIp".equals(attributeSignature.getType())) {
      signatureId =
          attributeSignature.getUserAgent().getAgentHash1() + Constants.FIELD_DELIM
              + attributeSignature
              .getUserAgent().getAgentHash2() + Constants.FIELD_DELIM + attributeSignature.getIp();

    } else if ("ip".equals(attributeSignature.getType())) {
      signatureId = attributeSignature.getIp().toString();
    }

    if (signature == null) {
      signature = new ConcurrentHashMap<>();
      attributeBroadcastStatus.put(attributeSignature.getType(), signature);
    }

    if (isGeneration) {
      for (int botFlag : botFlags) {
        if (signature.get(signatureId) != null) {
          if (signature.get(signatureId).containsKey(botFlag) && category == 2) {
            Long[] durationOld = signature.get(signatureId).get(botFlag);
            durationOld[1] = generationTime;
            signature.get(signatureId).put(botFlag, durationOld);
          } else if (!signature.get(signatureId).containsKey(botFlag) && category == 1) {
            Long[] duration = {generationTime, expirationTime};
            signature.get(signatureId).put(botFlag, duration);
          } else if (signature.get(signatureId).containsKey(botFlag) && category == 1) {
            Long[] durationOld = signature.get(signatureId).get(botFlag);
            if (durationOld[0] > generationTime) {
              durationOld[0] = generationTime;
            }
            if (durationOld[1] < expirationTime) {
              durationOld[1] = expirationTime;
            }
            signature.get(signatureId).put(botFlag, durationOld);
          }
        } else {
          Map<Integer, Long[]> newBotFlagStatus = new ConcurrentHashMap<>();
          Long[] duration = {generationTime, expirationTime};
          newBotFlagStatus.put(botFlag, duration);
          signature.put(signatureId, newBotFlagStatus);
          if (attributeSignature.getType().contains("ip")) {
            ipIncCounter.inc();
            ipCounter.inc();
          } else if (attributeSignature.getType().contains("agent")) {
            agentIncCounter.inc();
            agentCounter.inc();
          } else if (attributeSignature.getType().contains("agentIp")) {
            agentIpIncCounter.inc();
            agentIpCounter.inc();
          }
        }
      }
    } else {
      Map<Integer, Long[]> signatureBotFlagStatus = signature.get(signatureId);
      if (signatureBotFlagStatus != null) {
        for (int botFlag : botFlags) {
          if (signatureBotFlagStatus.containsKey(botFlag)) {
            if (expirationTime >= signatureBotFlagStatus.get(botFlag)[1]) {
              signatureBotFlagStatus.remove(botFlag);
              if (signatureBotFlagStatus.size() == 0) {
                if (attributeSignature.getType().contains("ip")) {
                  ipDecCounter.inc();
                  ipCounter.dec();
                } else if (attributeSignature.getType().contains("agent")) {
                  agentDecCounter.inc();
                  agentCounter.dec();
                } else if (attributeSignature.getType().contains("agentIp")) {
                  agentIpDecCounter.inc();
                  agentIpCounter.dec();
                }
                signature.remove(signatureId);
              }
            }
          }
        }
        if (signature.size() == 0) {
          attributeBroadcastStatus.remove(attributeSignature.getType());
        }
      }
    }


  }

  @Override
  public void open(Configuration parameters) throws Exception {
     Set<String> agentWhiteListOrigin= FlinkEnvUtils.getSet(Property.AGENT_WHILTELIST);
    agentWhiteList = new CopyOnWriteArraySet<>();

     for(String agent :agentWhiteListOrigin){
       long[] long4AgentHash = TypeTransformUtil
               .md522Long(TypeTransformUtil.getMD5(agent));
       agentWhiteList.add(long4AgentHash[0] + Constants.FIELD_DELIM + long4AgentHash[1]);
     }

    ipCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("ip signature count");
    agentCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("agent signature count");
    agentIpCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("agentIp signature count");
    ipIncCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("ip signature inc count");
    ipDecCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("ip signature dec count");
    agentIncCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("agent signature inc count");
    agentDecCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("agent signature dec count");
    agentIpIncCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("agentIp signature inc count");
    agentIpDecCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(Constants.SOJ_METRICS_GROUP)
            .counter("agentIp signature dec count");
  }
}