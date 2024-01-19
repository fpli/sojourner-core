package com.ebay.sojourner.rt.pipeline;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.RheosHeader;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import com.ebay.sojourner.flink.connector.hdfs.SessionMetricsDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.hdfs.HdfsConnectorFactory;
import com.ebay.sojourner.flink.connector.hdfs.SojSessionDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.SojSerializableTimestampAssigner;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventDeserializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventKafkaDeserializationSchemaWrapper;
import com.ebay.sojourner.flink.function.SessionMetricsTimestampTransMapFunction;
import com.ebay.sojourner.flink.function.map.SojSessionTimestampTransMapFunction;
import com.ebay.sojourner.flink.state.MapStateDesc;
import com.ebay.sojourner.flink.window.CompositeTrigger;
import com.ebay.sojourner.flink.window.MidnightOpenSessionTrigger;
import com.ebay.sojourner.flink.window.OnElementEarlyFiringTrigger;
import com.ebay.sojourner.flink.window.SojEventTimeSessionWindows;
import com.ebay.sojourner.rt.broadcast.AttributeBroadcastProcessFunctionForDetectable;
import com.ebay.sojourner.rt.metric.AgentIpMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.metric.AgentMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.metric.EventMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.metric.IpMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.metric.RTPipelineMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.operator.attribute.AgentAttributeAgg;
import com.ebay.sojourner.rt.operator.attribute.AgentIpAttributeAgg;
import com.ebay.sojourner.rt.operator.attribute.AgentIpAttributeAggSliding;
import com.ebay.sojourner.rt.operator.attribute.AgentIpSignatureWindowProcessFunction;
import com.ebay.sojourner.rt.operator.attribute.AgentIpWindowProcessFunction;
import com.ebay.sojourner.rt.operator.attribute.AgentWindowProcessFunction;
import com.ebay.sojourner.rt.operator.attribute.IpAttributeAgg;
import com.ebay.sojourner.rt.operator.attribute.IpWindowProcessFunction;
import com.ebay.sojourner.rt.operator.event.DetectableEventMapFunction;
import com.ebay.sojourner.rt.operator.event.EventDataStreamBuilder;
import com.ebay.sojourner.rt.operator.event.OpenSessionFilterFunction;
import com.ebay.sojourner.rt.operator.event.UbiEventMapWithStateFunction;
import com.ebay.sojourner.rt.operator.event.UbiEventToSojEventMapFunction;
import com.ebay.sojourner.rt.operator.event.UbiEventToSojEventProcessFunction;
import com.ebay.sojourner.rt.operator.metrics.UbiSessionToSessionMetricsProcessFunction;
import com.ebay.sojourner.rt.operator.session.DetectableSessionMapFunction;
import com.ebay.sojourner.rt.operator.session.UbiSessionAgg;
import com.ebay.sojourner.rt.operator.session.UbiSessionToSessionCoreMapFunction;
import com.ebay.sojourner.rt.operator.session.UbiSessionToSojSessionProcessFunction;
import com.ebay.sojourner.rt.operator.session.UbiSessionWindowProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorHelper;
import org.apache.flink.types.Either;

import java.time.Duration;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN;
import static com.ebay.sojourner.flink.common.DataCenter.LVS;
import static com.ebay.sojourner.flink.common.DataCenter.RNO;
import static com.ebay.sojourner.flink.common.DataCenter.SLC;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

public class SojournerRTJobSinkHdfs {

  public static void main(String[] args) throws Exception {

    // 0.0 Prepare execution environment
    // 0.1 UBI configuration
    // 0.2 Flink configuration
    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    // 1. Rheos Consumer
    // 1.1 Consume RawEvent from Rheos PathFinder topic
    // 1.2 Assign timestamps and emit watermarks.
    SourceDataStreamBuilder<RawEvent> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<RawEvent> rawEventDataStreamForRNO = dataStreamBuilder
        .dc(RNO)
        .operatorName(getString(Property.SOURCE_OPERATOR_NAME_RNO))
        .uid(getString(Property.SOURCE_UID_RNO))
        .slotGroup(getString(Property.SOURCE_EVENT_RNO_SLOT_SHARE_GROUP))
        .outOfOrderlessInMin(getInteger(FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN))
        .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
        .idleSourceTimeout(getInteger(Property.FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN))
        .build(new RawEventKafkaDeserializationSchemaWrapper(
            FlinkEnvUtils.getSet(Property.FILTER_GUID_SET),
            new RawEventDeserializationSchema(
                FlinkEnvUtils.getString(Property.RHEOS_KAFKA_REGISTRY_URL))));
    DataStream<RawEvent> rawEventDataStreamForSLC = dataStreamBuilder
        .dc(SLC)
        .operatorName(getString(Property.SOURCE_OPERATOR_NAME_SLC))
        .uid(getString(Property.SOURCE_UID_SLC))
        .slotGroup(getString(Property.SOURCE_EVENT_SLC_SLOT_SHARE_GROUP))
        .outOfOrderlessInMin(getInteger(FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN))
        .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
        .idleSourceTimeout(getInteger(Property.FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN))
        .build(new RawEventKafkaDeserializationSchemaWrapper(
            FlinkEnvUtils.getSet(Property.FILTER_GUID_SET),
            new RawEventDeserializationSchema(
                FlinkEnvUtils.getString(Property.RHEOS_KAFKA_REGISTRY_URL))));
    DataStream<RawEvent> rawEventDataStreamForLVS = dataStreamBuilder
        .dc(LVS)
        .operatorName(getString(Property.SOURCE_OPERATOR_NAME_LVS))
        .uid(getString(Property.SOURCE_UID_LVS))
        .slotGroup(getString(Property.SOURCE_EVENT_LVS_SLOT_SHARE_GROUP))
        .outOfOrderlessInMin(getInteger(FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN))
        .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
        .idleSourceTimeout(getInteger(Property.FLINK_APP_IDLE_SOURCE_TIMEOUT_IN_MIN))
        .build(new RawEventKafkaDeserializationSchemaWrapper(
            FlinkEnvUtils.getSet(Property.FILTER_GUID_SET),
            new RawEventDeserializationSchema(
                FlinkEnvUtils.getString(Property.RHEOS_KAFKA_REGISTRY_URL))));

    // 2. Event Operator
    // 2.1 Parse and transform RawEvent to UbiEvent
    // 2.2 Event level bot detection via bot rule
    DataStream<UbiEvent> ubiEventDataStreamForLVS = EventDataStreamBuilder
        .build(rawEventDataStreamForLVS, LVS);
    DataStream<UbiEvent> ubiEventDataStreamForSLC = EventDataStreamBuilder
        .build(rawEventDataStreamForSLC, SLC);
    DataStream<UbiEvent> ubiEventDataStreamForRNO = EventDataStreamBuilder
        .build(rawEventDataStreamForRNO, RNO);

    // union ubiEvent from SLC/RNO/LVS
    DataStream<UbiEvent> ubiEventDataStream = ubiEventDataStreamForLVS
        .union(ubiEventDataStreamForSLC)
        .union(ubiEventDataStreamForRNO);

    // refine windowsoperator
    // 3. Session Operator
    // 3.1 Session window
    // 3.2 Session indicator accumulation
    // 3.3 Session Level bot detection (via bot rule & signature)
    // 3.4 Event level bot detection (via session flag)
    SingleOutputStreamOperator<UbiSession> ubiSessionDataStream =
        ubiEventDataStream
            .keyBy("guid")
            .window(SojEventTimeSessionWindows.withGapAndMaxDuration(Time.minutes(30),
                Time.hours(24)))
            .trigger(CompositeTrigger.Builder.create().trigger(EventTimeTrigger.create())
                .trigger(MidnightOpenSessionTrigger
                    .of(Time.hours(7))).build())
            .sideOutputLateData(OutputTagConstants.lateEventOutputTag)
            .aggregate(new UbiSessionAgg(), new UbiSessionWindowProcessFunction());

    WindowOperatorHelper.enrichWindowOperator(
        (OneInputTransformation) ubiSessionDataStream.getTransformation(),
        new UbiEventMapWithStateFunction(),
        OutputTagConstants.mappedEventOutputTag);

    ubiSessionDataStream
        .setParallelism(getInteger(Property.SESSION_PARALLELISM))
        .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
        .name("Session Operator")
        .uid("session-operator");

    DataStream<UbiEvent> ubiEventWithSessionId =
        ubiSessionDataStream.getSideOutput(OutputTagConstants.mappedEventOutputTag);

    DataStream<UbiEvent> latedStream =
        ubiSessionDataStream.getSideOutput(OutputTagConstants.lateEventOutputTag);

    // ubiSession to SessionCore
    DataStream<SessionCore> sessionCoreDataStream =
        ubiSessionDataStream
            .filter(new OpenSessionFilterFunction())
            .setParallelism(getInteger(Property.SESSION_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("UbiSession Open Filter") //add opensession filter name
            .uid("ubisession-open-filter") //add opensession filter uid
            .map(new UbiSessionToSessionCoreMapFunction())
            .setParallelism(getInteger(Property.SESSION_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("UbiSession To SessionCore")
            .uid("ubisession-to-sessioncore");

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
            .setParallelism(getInteger(Property.PRE_AGENT_IP_PARALLELISM))
            .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
            .name("Attribute Operator (Agent+IP Pre-Aggregation)")
            .uid("attribute-operator-pre-aggregation");

    DataStream<BotSignature> agentIpSignatureDataStream = agentIpAttributeDatastream
        .keyBy("agent", "clientIp")
        .window(SlidingEventTimeWindows.of(
            Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(
            new AgentIpAttributeAggSliding(),
            new AgentIpSignatureWindowProcessFunction())
        .setParallelism(getInteger(Property.AGENT_IP_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name("Attribute Operator (Agent+IP)")
        .uid("attribute-operator-agent-ip");

    DataStream<BotSignature> agentSignatureDataStream = agentIpAttributeDatastream
        .keyBy("agent")
        .window(SlidingEventTimeWindows.of(
            Time.hours(24), Time.hours(12), Time.hours(7)))
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
        .setParallelism(getInteger(Property.AGENT_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name("Attribute Operator (Agent)")
        .uid("attribute-operator-agent");

    DataStream<BotSignature> ipSignatureDataStream = agentIpAttributeDatastream
        .keyBy("clientIp")
        .window(SlidingEventTimeWindows.of(
            Time.hours(24), Time.hours(12), Time.hours(7))) // sliding  to 3hours
        .trigger(OnElementEarlyFiringTrigger.create())
        .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
        .setParallelism(getInteger(Property.IP_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name("Attribute Operator (IP)")
        .uid("attribute-operator-ip");

    // union attribute signature for broadcast
    DataStream<BotSignature> attributeSignatureDataStream = agentIpSignatureDataStream
        .union(agentSignatureDataStream)
        .union(ipSignatureDataStream);

    // attribute signature broadcast
    BroadcastStream<BotSignature> attributeSignatureBroadcastStream =
        attributeSignatureDataStream.broadcast(MapStateDesc.attributeSignatureDesc);

    // transform ubiEvent,ubiSession to same type and union
    DataStream<Either<UbiEvent, UbiSession>> ubiSessionTransDataStream =
        ubiSessionDataStream
            .map(new DetectableSessionMapFunction())
            .setParallelism(getInteger(Property.SESSION_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("Transform UbiSession for Union")
            .uid("transform-ubisession-for-union");

    DataStream<Either<UbiEvent, UbiSession>> ubiEventTransDataStream =
        ubiEventWithSessionId
            .map(new DetectableEventMapFunction())
            .setParallelism(getInteger(Property.SESSION_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("Transform UbiEvent for Union")
            .uid("transform-ubievent-for-union");

    DataStream<Either<UbiEvent, UbiSession>> detectableDataStream =
        ubiSessionTransDataStream.union(ubiEventTransDataStream);

    // connect ubiEvent,ubiSession DataStream and broadcast Stream
    SingleOutputStreamOperator<UbiEvent> signatureBotDetectionForEvent =
        detectableDataStream.rescale().connect(attributeSignatureBroadcastStream)
            .process(
                new AttributeBroadcastProcessFunctionForDetectable(
                    OutputTagConstants.sessionOutputTag))
            .setParallelism(getInteger(Property.BROADCAST_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("Signature Bot Detector")
            .uid("signature-bot-detector");

    DataStream<UbiSession> signatureBotDetectionForSession =
        signatureBotDetectionForEvent.getSideOutput(OutputTagConstants.sessionOutputTag);

    // ubiEvent to sojEvent
    SingleOutputStreamOperator<SojEvent> sojEventWithSessionId =
        signatureBotDetectionForEvent
            .process(new UbiEventToSojEventProcessFunction(
                OutputTagConstants.botEventOutputTag))
            .setParallelism(getInteger(Property.BROADCAST_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("UbiEvent to SojEvent")
            .uid("ubievent-to-sojevent");

    DataStream<SojEvent> botSojEventStream = sojEventWithSessionId
        .getSideOutput(OutputTagConstants.botEventOutputTag);

    // ubiSession to sojSession
    final RheosHeader rheosHeader = new RheosHeader(
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            "sojourner event id",
            "sojourner test"
    );
    SingleOutputStreamOperator<SojSession> sojSessionStream =
        signatureBotDetectionForSession
            .process(
                new UbiSessionToSojSessionProcessFunction(
                    OutputTagConstants.botSessionOutputTag))
            .setParallelism(getInteger(Property.BROADCAST_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("UbiSession to SojSession")
            .uid("ubisession-to-sojsession")
            .map(
                (MapFunction<SojSession, SojSession>) sojSession -> {
                  sojSession.setRheosHeader(rheosHeader);
                  return sojSession;
            })
            .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
            .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
            .name("SojSession Set Rheos Header Null")
            .uid("sojsession-set-rheos-header-null");

    // assgin watermark
    DataStream<SojSession> assignedWatermarkSojSessionStream = sojSessionStream
          .assignTimestampsAndWatermarks(
                  WatermarkStrategy
                          .<SojSession>forBoundedOutOfOrderness(Duration.ofMinutes(
                                  FlinkEnvUtils.getInteger(Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN)))
                          .withTimestampAssigner(new SojSerializableTimestampAssigner<>()))
          .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
          .name("Soj Session Assign Watermark")
          .uid("assign-watermark-soj-session");

    // unix timestamp to sojourner timestamp
    DataStream<SojSession> finalSojSessionDataStream = assignedWatermarkSojSessionStream
          .map(new SojSessionTimestampTransMapFunction())
          .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
          .name("Session Unix Timestamp To Soj Timestamp")
          .uid("session-unix-timestamp-to-soj-timestamp");

    // extract botMetrics from ubiSession
    SingleOutputStreamOperator<SessionMetrics> sessionMetricsStream = signatureBotDetectionForSession
          .process(new UbiSessionToSessionMetricsProcessFunction())
          .setParallelism(getInteger(Property.BROADCAST_PARALLELISM))
          .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
          .name("UbiSession to sessionMetrics")
          .uid("ubisession-to-session-metrics")
          .map((MapFunction<SessionMetrics, SessionMetrics>) sessionMetrics -> {
            sessionMetrics.setRheosHeader(rheosHeader);
            return sessionMetrics;
          })
          .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
          .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
          .name("SojSession Set Rheos Header Null")
          .uid("sojsession-set-rheos-header-null");

    // assign watermark
    DataStream<SessionMetrics> assignedWatermarkSessionMetricsStream = sessionMetricsStream
          .assignTimestampsAndWatermarks(
                  WatermarkStrategy
                          .<SessionMetrics>forBoundedOutOfOrderness(Duration.ofMinutes(
                                  FlinkEnvUtils.getInteger(Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN)))
                          .withTimestampAssigner(new SojSerializableTimestampAssigner<>()))
          .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
          .name("Assign Watermark Session Metrics")
          .uid("assign-watermark-session-metrics");

    // unix timestamp to sojourner timestamp
    DataStream<SessionMetrics> finalSessionMetricsDataStream = assignedWatermarkSessionMetricsStream
          .map(new SessionMetricsTimestampTransMapFunction())
          .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
          .name("Metrics Unix Timestamp To Soj Timestamp")
          .uid("metrics-unix-timestamp-to-soj-timestamp");

    // 5. Load data to file system for batch processing
    // 5.1 IP Signature
    // 5.2 Sessions (ended)
    // 5.3 Events (with session ID & bot flags)
    // 5.4 Events late

    // HDFS sink for bot and nonbot sojsession
    finalSojSessionDataStream
            .addSink(HdfsConnectorFactory.createWithParquet(
                    getString(Property.FLINK_APP_SINK_HDFS_SESSION_NONBOT_PATH), SojSession.class,
                    new SojSessionDateTimeBucketAssigner()))
            .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
            .name("SojSession Sink HDFS")
            .uid("sojsession-sink-hdfs");

    // HDFS sink for bot and nonbot metrics
    finalSessionMetricsDataStream
            .addSink(HdfsConnectorFactory.createWithParquet(
                    getString(Property.FLINK_APP_SINK_HDFS_METRICS_NONBOT_PATH), SessionMetrics.class,
                    new SessionMetricsDateTimeBucketAssigner()))
            .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
            .name("Bot metrics Sink HDFS")
            .uid("bot-metrics-sink-hdfs");

    // TODO: sink for non-bot sojevent to HDFS
    sojEventWithSessionId
        .addSink(new DiscardingSink<>())
        .setParallelism(getInteger(Property.BROADCAST_PARALLELISM))
        .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
        .name("Nonbot SojEvent")
        .uid("nonbot-sojevent-sink-hdfs");

    // TODO: sink for bot sojevent to HDFS
    botSojEventStream
        .addSink(new DiscardingSink<>())
        .setParallelism(getInteger(Property.BROADCAST_PARALLELISM))
        .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
        .name("Bot SojEvent")
        .uid("bot-sojevent-sink-hdfs");

    // metrics collector for end to end
    signatureBotDetectionForEvent
        .process(new RTPipelineMetricsCollectorProcessFunction(
            FlinkEnvUtils.getInteger(Property.METRIC_WINDOW_SIZE)))
        .setParallelism(getInteger(Property.METRICS_PARALLELISM))
        .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
        .name("Pipeline Metrics Collector")
        .uid("pipeline-metrics-collector");

    // metrics collector for event rules hit
    signatureBotDetectionForEvent
        .process(new EventMetricsCollectorProcessFunction())
        .setParallelism(getInteger(Property.METRICS_PARALLELISM))
        .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
        .name("Event Metrics Collector")
        .uid("event-metrics-collector");

    // metrics collector for signature generation or expiration
    agentIpSignatureDataStream
        .process(new AgentIpMetricsCollectorProcessFunction())
        .setParallelism(getInteger(Property.AGENT_IP_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name("AgentIp Metrics Collector")
        .uid("agent-ip-metrics-collector");

    agentSignatureDataStream
        .process(new AgentMetricsCollectorProcessFunction())
        .setParallelism(getInteger(Property.AGENT_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name("Agent Metrics Collector")
        .uid("agent-metrics-id");

    ipSignatureDataStream
        .process(new IpMetricsCollectorProcessFunction())
        .setParallelism(getInteger(Property.IP_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name("Ip Metrics Collector")
        .uid("ip-metrics-id");

    // No sink for signature
    agentIpSignatureDataStream
        .addSink(new DiscardingSink<>())
        .setParallelism(getInteger(Property.DEFAULT_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name(String.format("%s Signature", Constants.AGENTIP))
        .uid(String.format("signature-%s-sink", Constants.AGENTIP));
    agentSignatureDataStream
        .addSink(new DiscardingSink<>())
        .setParallelism(getInteger(Property.DEFAULT_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name(String.format("%s Signature", Constants.AGENT))
        .uid(String.format("signature-%s-sink", Constants.AGENT));

    ipSignatureDataStream
        .addSink(new DiscardingSink<>())
        .setParallelism(getInteger(Property.DEFAULT_PARALLELISM))
        .slotSharingGroup(getString(Property.CROSS_SESSION_SLOT_SHARE_GROUP))
        .name(String.format("%s Signature", Constants.IP))
        .uid(String.format("signature-%s-sink", Constants.IP));

    // No sink for late event
    DataStream<SojEvent> lateSojEventStream = latedStream
        .map(new UbiEventToSojEventMapFunction())
        .setParallelism(getInteger(Property.SESSION_PARALLELISM))
        .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
        .name("Late UbiEvent to SojEvent")
        .uid("late-ubievent-to-sojevent");

    lateSojEventStream.addSink(new DiscardingSink<>())
        .setParallelism(getInteger(Property.SESSION_PARALLELISM))
        .slotSharingGroup(getString(Property.SESSION_SLOT_SHARE_GROUP))
        .name("Late SojEvent")
        .uid("late-sojevent-sink");

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
  }
}
