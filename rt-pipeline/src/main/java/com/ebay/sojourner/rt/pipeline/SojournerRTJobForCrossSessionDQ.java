package com.ebay.sojourner.rt.pipeline;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.IntermediateSession;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.env.FlinkEnvUtils;
import com.ebay.sojourner.flink.common.state.MapStateDesc;
import com.ebay.sojourner.flink.common.util.OutputTagUtil;
import com.ebay.sojourner.flink.common.window.OnElementEarlyFiringTrigger;
import com.ebay.sojourner.flink.connectors.hdfs.HdfsConnectorFactory;
import com.ebay.sojourner.flink.connectors.kafka.KafkaSourceFunction;
import com.ebay.sojourner.rt.common.broadcast.CrossSessionDQBroadcastProcessFunction;
import com.ebay.sojourner.rt.operators.attribute.AgentAttributeAgg;
import com.ebay.sojourner.rt.operators.attribute.AgentIpAttributeAgg;
import com.ebay.sojourner.rt.operators.attribute.AgentIpAttributeAggSliding;
import com.ebay.sojourner.rt.operators.attribute.AgentIpSignatureWindowProcessFunction;
import com.ebay.sojourner.rt.operators.attribute.AgentIpWindowProcessFunction;
import com.ebay.sojourner.rt.operators.attribute.AgentWindowProcessFunction;
import com.ebay.sojourner.rt.operators.attribute.GuidAttributeAgg;
import com.ebay.sojourner.rt.operators.attribute.GuidWindowProcessFunction;
import com.ebay.sojourner.rt.operators.attribute.IpAttributeAgg;
import com.ebay.sojourner.rt.operators.attribute.IpWindowProcessFunction;
import com.ebay.sojourner.rt.operators.session.IntermediateSessionToSessionCoreMapFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SojournerRTJobForCrossSessionDQ {

  public static void main(String[] args) throws Exception {

    // 0.0 Prepare execution environment
    // 0.1 UBI configuration
    // 0.2 Flink configuration
    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    // kafka source for copy
    DataStream<IntermediateSession> intermediateSessionDataStream =
        executionEnvironment
            .addSource(KafkaSourceFunction.buildSource(
                FlinkEnvUtils.getString(Property.KAFKA_CONSUMER_TOPIC),
                FlinkEnvUtils
                    .getListString(Property.KAFKA_CONSUMER_BOOTSTRAP_SERVERS_RNO),
                FlinkEnvUtils.getString(Property.KAFKA_CONSUMER_GROUP_ID),
                IntermediateSession.class))
            .setParallelism(FlinkEnvUtils.getInteger(Property.SOURCE_PARALLELISM))
            .name("Rheos Kafka Consumer For Cross Session DQ")
            .uid("source-id");

    // intermediateSession to sessionCore
    DataStream<SessionCore> sessionCoreDS = intermediateSessionDataStream
        .map(new IntermediateSessionToSessionCoreMapFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.SESSION_PARALLELISM))
        .name("IntermediateSession To SessionCore")
        .uid("session-enhance-id");

    // cross session
    DataStream<AgentIpAttribute> agentIpAttributeDatastream = sessionCoreDS
        .keyBy("userAgent", "ip")
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .aggregate(new AgentIpAttributeAgg(), new AgentIpWindowProcessFunction())
        .name("Attribute Operator (Agent+IP Pre-Aggregation)")
        .setParallelism(FlinkEnvUtils.getInteger(Property.PRE_AGENT_IP_PARALLELISM))
        .uid("pre-agent-ip-id");

    DataStream<BotSignature> guidSignatureDataStream = sessionCoreDS
        .keyBy("guid")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new GuidAttributeAgg(), new GuidWindowProcessFunction())
        .name("Attribute Operator (GUID)")
        .setParallelism(FlinkEnvUtils.getInteger(Property.GUID_PARALLELISM))
        .uid("guid-id");

    DataStream<BotSignature> agentIpSignatureDataStream = agentIpAttributeDatastream
        .keyBy("agent", "clientIp")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(
            new AgentIpAttributeAggSliding(), new AgentIpSignatureWindowProcessFunction())
        .name("Attribute Operator (Agent+IP)")
        .setParallelism(FlinkEnvUtils.getInteger(Property.AGENT_IP_PARALLELISM))
        .uid("agent-ip-id");

    DataStream<BotSignature> agentSignatureDataStream = agentIpAttributeDatastream
        .keyBy("agent")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
        .name("Attribute Operator (Agent)")
        .setParallelism(FlinkEnvUtils.getInteger(Property.AGENT_PARALLELISM))
        .uid("agent-id");

    DataStream<BotSignature> ipSignatureDataStream = agentIpAttributeDatastream
        .keyBy("clientIp")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
        .name("Attribute Operator (IP)")
        .setParallelism(FlinkEnvUtils.getInteger(Property.IP_PARALLELISM))
        .uid("ip-id");

    // union attribute signature for broadcast
    DataStream<BotSignature> attributeSignatureDataStream = agentIpSignatureDataStream
        .union(agentSignatureDataStream)
        .union(ipSignatureDataStream)
        .union(guidSignatureDataStream);

    // attribute signature broadcast
    BroadcastStream<BotSignature> attributeSignatureBroadcastStream =
        attributeSignatureDataStream.broadcast(MapStateDesc.attributeSignatureDesc);

    // connect broadcast
    SingleOutputStreamOperator<IntermediateSession> intermediateSessionWithSignature =
        intermediateSessionDataStream
            .connect(attributeSignatureBroadcastStream)
            .process(new CrossSessionDQBroadcastProcessFunction(OutputTagUtil.sessionOutputTag))
            .setParallelism(FlinkEnvUtils.getInteger(Property.BROADCAST_PARALLELISM))
            .name("Signature Bot Detector")
            .uid("signature-detection-id");

    attributeSignatureDataStream
        .addSink(HdfsConnectorFactory
            .createWithParquet(FlinkEnvUtils.getString(Property.HDFS_PATH_PARENT) +
                    FlinkEnvUtils.getString(Property.HDFS_PATH_SIGNATURES),
                BotSignature.class))
        .setParallelism(FlinkEnvUtils.getInteger(Property.AGENT_IP_PARALLELISM))
        .name("BotSignatures")
        .uid("bot-signatures-sink-id");

    intermediateSessionWithSignature
        .addSink(HdfsConnectorFactory.createWithParquet(
            FlinkEnvUtils.getString(Property.HDFS_PATH_PARENT) +
                FlinkEnvUtils.getString(Property.HDFS_PATH_INTERMEDIATE_SESSION),
            IntermediateSession.class))
        .setParallelism(FlinkEnvUtils.getInteger(Property.BROADCAST_PARALLELISM))
        .name("IntermediateSession")
        .uid("intermediate-session-sink-id");

    // Submit this job
    FlinkEnvUtils
        .execute(executionEnvironment, FlinkEnvUtils.getString(Property.NAME_DATA_QUALITY));
  }
}
