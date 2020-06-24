package com.ebay.sojourner.rt.common.util;

import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.SignatureInfo;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.env.FlinkEnvUtils;
import com.ebay.sojourner.flink.connectors.kafka.KafkaProducerFactory;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;

public class SignatureUtils {

  public static Set<Integer> generateNewSignature(Map<Integer, Integer> signatureStates) {

    LinkedHashSet<Integer> newGenerateSignatures = new LinkedHashSet<>();

    if (signatureStates.size() > 0) {
      for (Map.Entry<Integer, Integer> signatureState : signatureStates.entrySet()) {
        newGenerateSignatures.add(signatureState.getKey());

      }
    }

    return newGenerateSignatures;
  }

  public static Set<Integer> setBotFlags(Set<Integer> sourceSet, Set<Integer> targetSet) {
    targetSet.clear();
    targetSet.addAll(sourceSet);
    return targetSet;
  }

  public static void signatureMetricsCollection(Map<String, Counter> signatureCounterNameMap,
      String signatureId, Boolean isGeneration) {

    if (signatureCounterNameMap.get(signatureId + Constants.GENERATION_PREFFIX) != null
        || signatureCounterNameMap.get(signatureId + Constants.EXPIRATION_PREFFIX) != null) {
      if (isGeneration) {
        Counter gCounter = signatureCounterNameMap.get(signatureId + Constants.GENERATION_PREFFIX);
        gCounter.inc();
      } else {
        Counter eCounter = signatureCounterNameMap.get(signatureId + Constants.EXPIRATION_PREFFIX);
        eCounter.inc();
      }
    }
  }

  public static void buildSignatureKafkaSink(DataStream<BotSignature> dataStream, String topic,
      String signatureId, String slotGroup, String messageKey) {

    dataStream
        .addSink(KafkaProducerFactory.getProducer(
            topic,
            FlinkEnvUtils.getListString(Property.KAFKA_PRODUCER_BOOTSTRAP_SERVERS_RNO),
            messageKey,
            BotSignature.class))
        .setParallelism(FlinkEnvUtils.getInteger(Property.DEFAULT_PARALLELISM))
        .slotSharingGroup(slotGroup)
        .name(String.format("%s Signature", signatureId))
        .uid(String.format("signature-%s-sink-id", signatureId));

  }

  public static void updateSignatureStatus(Map<Integer, SignatureInfo> signatureStatus,
      Set<Integer> botFlags) {

    for (Map.Entry<Integer, SignatureInfo> entry : signatureStatus.entrySet()) {
      if (!entry.getValue().isSent()) {
        entry.getValue().setSent(true);
        signatureStatus.put(entry.getKey(), entry.getValue());
      }

      if (!botFlags.contains(entry.getKey())) {
        if (entry.getValue().getType() == 1) {
          entry.getValue().setType(2);
          entry.getValue().setSent(false);
          signatureStatus.put(entry.getKey(), entry.getValue());
        }
      }
    }

    SignatureInfo signatureInfo;
    for (Integer botFlag : botFlags) {
      if (!signatureStatus.containsKey(botFlag)) {
        signatureInfo = new SignatureInfo();
        signatureInfo.setType(1);
        signatureInfo.setSent(false);
        signatureStatus.put(botFlag, signatureInfo);
      }
    }
  }
}
