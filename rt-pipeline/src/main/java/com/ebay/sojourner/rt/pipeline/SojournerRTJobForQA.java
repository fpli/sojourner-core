package com.ebay.sojourner.rt.pipeline;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.env.FlinkEnvUtils;
import com.ebay.sojourner.flink.common.state.MapStateDesc;
import com.ebay.sojourner.flink.common.util.DataCenter;
import com.ebay.sojourner.flink.common.util.OutputTagUtil;
import com.ebay.sojourner.flink.common.window.OnElementEarlyFiringTrigger;
import com.ebay.sojourner.flink.connectors.kafka.KafkaConnectorFactory;
import com.ebay.sojourner.flink.connectors.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.rt.common.broadcast.AttributeBroadcastProcessFunctionForDetectable;
import com.ebay.sojourner.rt.common.metrics.AgentIpMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.common.metrics.AgentMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.common.metrics.EventMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.common.metrics.GuidMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.common.metrics.IpMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.common.metrics.PipelineMetricsCollectorProcessFunction;
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
import com.ebay.sojourner.rt.operators.event.DetectableEventMapFunction;
import com.ebay.sojourner.rt.operators.event.EventDataStreamBuilder;
import com.ebay.sojourner.rt.operators.event.UbiEventMapWithStateFunction;
import com.ebay.sojourner.rt.operators.event.UbiEventToSojEventMapFunction;
import com.ebay.sojourner.rt.operators.session.DetectableSessionMapFunction;
import com.ebay.sojourner.rt.operators.session.UbiSessionAgg;
import com.ebay.sojourner.rt.operators.session.UbiSessionToSessionCoreMapFunction;
import com.ebay.sojourner.rt.operators.session.UbiSessionToSojSessionMapFunction;
import com.ebay.sojourner.rt.operators.session.UbiSessionWindowProcessFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorHelper;
import org.apache.flink.types.Either;

public class SojournerRTJobForQA {

  public static void main(String[] args) throws Exception {

    // 0.0 Prepare execution environment
    // 0.1 UBI configuration
    // 0.2 Flink configuration
    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    // 1. Rheos Consumer
    // 1.1 Consume RawEvent from Rheos PathFinder topic
    // 1.2 Assign timestamps and emit watermarks.
    DataStream<RawEvent> rawEventDataStream = SourceDataStreamBuilder.build(
        executionEnvironment,
        FlinkEnvUtils.getString(Property.BEHAVIOR_PATHFINDER_TOPIC),
        FlinkEnvUtils.getListString(Property.BEHAVIOR_PATHFINDER_BOOTSTRAP_SERVERS_LVS),
        FlinkEnvUtils.getString(Property.BEHAVIOR_PATHFINDER_GROUP_ID_DEFAULT_LVS),
        DataCenter.LVS,
        FlinkEnvUtils.getInteger(Property.SOURCE_PARALLELISM),
        null,
        RawEvent.class);

    // 2. Event Operator
    // 2.1 Parse and transform RawEvent to UbiEvent
    // 2.2 Event level bot detection via bot rule
    DataStream<UbiEvent> ubiEventDataStream = EventDataStreamBuilder.build(
        rawEventDataStream,
        DataCenter.LVS,
        FlinkEnvUtils.getInteger(Property.EVENT_PARALLELISM),
        null);

    // refine windowsoperator
    // 3. Session Operator
    // 3.1 Session window
    // 3.2 Session indicator accumulation
    // 3.3 Session Level bot detection (via bot rule & signature)
    // 3.4 Event level bot detection (via session flag)
    SingleOutputStreamOperator<UbiSession> ubiSessionDataStream =
        ubiEventDataStream
            .keyBy("guid")
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .allowedLateness(Time.minutes(1))
            .sideOutputLateData(OutputTagUtil.lateEventOutputTag)
            .aggregate(new UbiSessionAgg(), new UbiSessionWindowProcessFunction());

    WindowOperatorHelper.enrichWindowOperator(
        (OneInputTransformation) ubiSessionDataStream.getTransformation(),
        new UbiEventMapWithStateFunction(),
        OutputTagUtil.mappedEventOutputTag);

    ubiSessionDataStream
        .setParallelism(FlinkEnvUtils.getInteger(Property.SESSION_PARALLELISM))
        .name("Session Operator")
        .uid("session-id");

    DataStream<UbiEvent> ubiEventWithSessionId =
        ubiSessionDataStream.getSideOutput(OutputTagUtil.mappedEventOutputTag);

    DataStream<UbiEvent> latedStream =
        ubiSessionDataStream.getSideOutput(OutputTagUtil.lateEventOutputTag);

    // ubiSession to sessionCore
    DataStream<SessionCore> sessionCoreDataStream = ubiSessionDataStream
        .map(new UbiSessionToSessionCoreMapFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.SESSION_PARALLELISM))
        .name("UbiSession To SessionCore")
        .uid("session-enhance-id");

    // 4. Attribute Operator
    // 4.1 Sliding window
    // 4.2 Attribute indicator accumulation
    // 4.3 Attribute level bot detection (via bot rule)
    // 4.4 Store bot signature
    DataStream<AgentIpAttribute> agentIpAttributeDatastream =
        sessionCoreDataStream
            .keyBy("userAgent", "ip")
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new AgentIpAttributeAgg(), new AgentIpWindowProcessFunction())
            .setParallelism(FlinkEnvUtils.getInteger(Property.PRE_AGENT_IP_PARALLELISM))
            .name("Attribute Operator (Agent+IP Pre-Aggregation)")
            .uid("pre-agent-ip-id");

    DataStream<BotSignature> guidSignatureDataStream = sessionCoreDataStream
        .keyBy("guid")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new GuidAttributeAgg(), new GuidWindowProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.GUID_PARALLELISM))
        .name("Attribute Operator (GUID)")
        .uid("guid-id");

    DataStream<BotSignature> agentIpSignatureDataStream = agentIpAttributeDatastream
        .keyBy("agent", "clientIp")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(
            new AgentIpAttributeAggSliding(), new AgentIpSignatureWindowProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.AGENT_IP_PARALLELISM))
        .name("Attribute Operator (Agent+IP)")
        .uid("agent-ip-id");

    DataStream<BotSignature> agentSignatureDataStream = agentIpAttributeDatastream
        .keyBy("agent")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.AGENT_PARALLELISM))
        .name("Attribute Operator (Agent)")
        .uid("agent-id");

    DataStream<BotSignature> ipSignatureDataStream = agentIpAttributeDatastream
        .keyBy("clientIp")
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.IP_PARALLELISM))
        .name("Attribute Operator (IP)")
        .uid("ip-id");

    // union attribute signature for broadcast
    DataStream<BotSignature> attributeSignatureDataStream = agentIpSignatureDataStream
        .union(agentSignatureDataStream)
        .union(ipSignatureDataStream)
        .union(guidSignatureDataStream);

    // attribute signature broadcast
    BroadcastStream<BotSignature> attributeSignatureBroadcastStream =
        attributeSignatureDataStream.broadcast(MapStateDesc.attributeSignatureDesc);

    // transform ubiEvent,ubiSession to same type and union
    DataStream<Either<UbiEvent, UbiSession>> ubiSessionTransDataStream = ubiSessionDataStream
        .map(new DetectableSessionMapFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.SESSION_PARALLELISM))
        .name("UbiSessionTransForBroadcast")
        .uid("session-broadcast-id");

    DataStream<Either<UbiEvent, UbiSession>> ubiEventTransDataStream = ubiEventWithSessionId
        .map(new DetectableEventMapFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.SESSION_PARALLELISM))
        .name("UbiEventTransForBroadcast")
        .uid("event-broadcast-id");

    DataStream<Either<UbiEvent, UbiSession>> detectableDataStream = ubiSessionTransDataStream
        .union(ubiEventTransDataStream);

    // connect ubiEvent,ubiSession DataStream and broadcast Stream
    SingleOutputStreamOperator<UbiEvent> signatureBotDetectionForEvent = detectableDataStream
        .connect(attributeSignatureBroadcastStream)
        .process(new AttributeBroadcastProcessFunctionForDetectable(OutputTagUtil.sessionOutputTag))
        .setParallelism(FlinkEnvUtils.getInteger(Property.BROADCAST_PARALLELISM))
        .name("Signature Bot Detector")
        .uid("signature-detection-id");

    DataStream<UbiSession> signatureBotDetectionForSession =
        signatureBotDetectionForEvent.getSideOutput(OutputTagUtil.sessionOutputTag);

    // UbiSession to SojSession
    DataStream<SojSession> sojSessionStream =
        signatureBotDetectionForSession
            .map(new UbiSessionToSojSessionMapFunction())
            .setParallelism(FlinkEnvUtils.getInteger(Property.BROADCAST_PARALLELISM))
            .name("UbiSession to SojSession")
            .uid("session-transform-id");

    // UbiEvent to SojEvent
    DataStream<SojEvent> sojEventWithSessionId =
        signatureBotDetectionForEvent
            .map(new UbiEventToSojEventMapFunction())
            .setParallelism(FlinkEnvUtils.getInteger(Property.BROADCAST_PARALLELISM))
            .name("UbiEvent to SojEvent")
            .uid("event-transform-id");

    // 5. Load data to file system for batch processing
    // 5.1 IP Signature
    // 5.2 Sessions (ended)
    // 5.3 Events (with session ID & bot flags)
    // 5.4 Events late

    // kafka sink for session
    sojSessionStream.addSink(KafkaConnectorFactory
        .createKafkaProducer(
            FlinkEnvUtils.getString(Property.BEHAVIOR_TRAFFICJAM_TOPIC_SESSION_NON_BOT),
            FlinkEnvUtils.getListString(Property.BEHAVIOR_TRAFFICJAM_BOOTSTRAP_SERVERS_DEFAULT),
            SojSession.class,
            FlinkEnvUtils.getString(Property.BEHAVIOR_MESSAGE_KEY_SESSION)))
        .setParallelism(FlinkEnvUtils.getInteger(Property.BROADCAST_PARALLELISM))
        .name("SojSession")
        .uid("session-sink-id");

    // kafka sink for event
    sojEventWithSessionId.addSink(KafkaConnectorFactory
        .createKafkaProducer(
            FlinkEnvUtils.getString(Property.BEHAVIOR_TRAFFICJAM_TOPIC_EVENT_NON_BOT),
            FlinkEnvUtils.getListString(Property.BEHAVIOR_TRAFFICJAM_BOOTSTRAP_SERVERS_DEFAULT),
            SojEvent.class,
            FlinkEnvUtils.getString(Property.BEHAVIOR_MESSAGE_KEY_EVENT)))
        .setParallelism(FlinkEnvUtils.getInteger(Property.BROADCAST_PARALLELISM))
        .name("SojEvent")
        .uid("event-sink-id");

    // metrics collector for end to end
    signatureBotDetectionForEvent
        .process(new PipelineMetricsCollectorProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.METRICS_PARALLELISM))
        .name("Pipeline Metrics Collector")
        .uid("pipeline-metrics-id");

    // metrics collector for event rules hit
    signatureBotDetectionForEvent
        .process(new EventMetricsCollectorProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.METRICS_PARALLELISM))
        .name("Event Metrics Collector")
        .uid("event-metrics-id");

    // metrics collector for signature generation or expiration
    guidSignatureDataStream
        .process(new GuidMetricsCollectorProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.GUID_PARALLELISM))
        .name("Guid Metrics Collector")
        .uid("guid-metrics-id");

    agentIpSignatureDataStream
        .process(new AgentIpMetricsCollectorProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.AGENT_IP_PARALLELISM))
        .name("AgentIp Metrics Collector")
        .uid("agentIp-metrics-id");

    agentSignatureDataStream
        .process(new AgentMetricsCollectorProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.AGENT_PARALLELISM))
        .name("Agent Metrics Collector")
        .uid("agent-metrics-id");

    ipSignatureDataStream
        .process(new IpMetricsCollectorProcessFunction())
        .setParallelism(FlinkEnvUtils.getInteger(Property.IP_PARALLELISM))
        .name("Ip Metrics Collector")
        .uid("ip-metrics-id");

    // late event sink
    latedStream
        .addSink(new DiscardingSink<>())
        .setParallelism(FlinkEnvUtils.getInteger(Property.SESSION_PARALLELISM))
        .name("Late Event")
        .uid("event-late-id");

    // Submit this job
    FlinkEnvUtils
        .execute(executionEnvironment, FlinkEnvUtils.getString(Property.NAME_FULL_PIPELINE));
  }
}
