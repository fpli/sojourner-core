package com.ebay.sojourner.rt.pipeline;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventDeserializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.RawEventKafkaDeserializationSchemaWrapper;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.BotSignatureKafkaRecordSerializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.SessionMetricsKeySerializerSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.SessionMetricsValueSerializerSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.SojEventKafkaRecordSerializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.SojSessionKeySerializerSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.SojSessionValueSerializerSchema;
import com.ebay.sojourner.flink.state.MapStateDesc;
import com.ebay.sojourner.flink.watermark.RawEventTimestampAssigner;
import com.ebay.sojourner.flink.window.CompositeTrigger;
import com.ebay.sojourner.flink.window.MidnightOpenSessionTrigger;
import com.ebay.sojourner.flink.window.OnElementEarlyFiringTrigger;
import com.ebay.sojourner.flink.window.SojEventTimeSessionWindows;
import com.ebay.sojourner.rt.broadcast.AttributeBroadcastProcessFunctionForDetectable;
import com.ebay.sojourner.rt.metric.AgentIpMetricsCollectorProcessFunction;
import com.ebay.sojourner.rt.metric.AgentMetricsCollectorProcessFunction;
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
import com.ebay.sojourner.rt.operator.event.EventMapFunction;
import com.ebay.sojourner.rt.operator.event.LargeMessageHandler;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorHelper;
import org.apache.flink.types.Either;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_WATERMARK_IDLE_SOURCE_TIMEOUT_IN_MIN;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_WATERMARK_MAX_OUT_OF_ORDERNESS_IN_MIN;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_REGISTRY_URL;
import static com.ebay.sojourner.flink.common.DataCenter.LVS;
import static com.ebay.sojourner.flink.common.DataCenter.RNO;
import static com.ebay.sojourner.flink.common.DataCenter.SLC;
import static org.apache.flink.streaming.api.windowing.time.Time.hours;
import static org.apache.flink.streaming.api.windowing.time.Time.minutes;

public class SojournerRTJob {

    public static void main(String[] args) throws Exception {
        // 1. Prepare Flink environment
        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // configs
        final Integer MAX_OUT_OF_ORDERNESS = flinkEnv.getInteger(FLINK_APP_WATERMARK_MAX_OUT_OF_ORDERNESS_IN_MIN);
        final Integer IDLE_SOURCE_TIMEOUT = flinkEnv.getInteger(FLINK_APP_WATERMARK_IDLE_SOURCE_TIMEOUT_IN_MIN);

        final Long LARGE_MESSAGE_MAX_BYTES = flinkEnv.getLong("flink.app.filter.large-message.max-bytes");
        final Integer SUB_URL_QUERY_STRING_LENGTH =
                flinkEnv.getInteger("flink.app.filter.large-message.sub-url-query-string-length");
        final Boolean TRUNCATE_URL_QUERY_STRING =
                flinkEnv.getBoolean("flink.app.filter.large-message.truncate-url-query-string");
        final Set<String> LARGE_MSG_PAGEID_MONITOR =
                flinkEnv.getStringSet("flink.app.filter.large-message.pageid-monitor", ",");


        final int PARALLELISM_SESSION = flinkEnv.getInteger("flink.app.parallelism.session");
        final int PARALLELISM_BROADCAST = flinkEnv.getInteger("flink.app.parallelism.broadcast");
        final int PARALLELISM_AGENT_IP = flinkEnv.getInteger("flink.app.parallelism.agent-ip");

        final String SLOT_GROUP_SOURCE_RNO = "source-rno";
        final String SLOT_GROUP_SOURCE_LVS = "source-lvs";
        final String SLOT_GROUP_SOURCE_SLC = "source-slc";
        final String SLOT_GROUP_SESSION = "session";
        final String SLOT_GROUP_CROSS_SESSION = "cross-session";
        final String SLOT_GROUP_EVENT_SINK = "event-sink";

        final String REGISTRY_URL = flinkEnv.getString(RHEOS_REGISTRY_URL);

        // event topics
        final String TOPIC_BOT_EVENT = flinkEnv.getString("flink.app.sink.kafka.topic.event.bot");
        final String TOPIC_NONBOT_EVENT = flinkEnv.getString("flink.app.sink.kafka.topic.event.non-bot");
        final String TOPIC_LATE_EVENT = flinkEnv.getString("flink.app.sink.kafka.topic.event.late");
        // session topics
        final String TOPIC_BOT_SESSION = flinkEnv.getString("flink.app.sink.kafka.topic.session.bot");
        final String TOPIC_NONBOT_SESSION = flinkEnv.getString("flink.app.sink.kafka.topic.session.non-bot");
        final String TOPIC_SESSION_METRICS = flinkEnv.getString("flink.app.sink.kafka.topic.session.metrics");
        // bot signature topics
        final String TOPIC_BOT_SIG_AGENT_IP =
                flinkEnv.getString("flink.app.sink.kafka.topic.bot-signature.agent-ip");
        final String TOPIC_BOT_SIG_AGENT = flinkEnv.getString("flink.app.sink.kafka.topic.bot-signature.agent");
        final String TOPIC_BOT_SIG_IP = flinkEnv.getString("flink.app.sink.kafka.topic.bot-signature.ip");

        // schema subject names
        final String SUBJECT_SOJEVENT = flinkEnv.getString("flink.app.sink.kafka.subject.event");
        final String SUBJECT_SOJSESSION = flinkEnv.getString("flink.app.sink.kafka.subject.session");
        final String SUBJECT_SESSION_METRICS = flinkEnv.getString("flink.app.sink.kafka.subject.session-metrics");

        final String SINK_KAFKA_BROKERS = flinkEnv.getSinkKafkaBrokers();
        final Properties KAFKA_PRODUCER_PROPS = flinkEnv.getKafkaProducerProps();

        final int metricWindow = 70000;

        // 2. Source & Filter & Event
        // 2.1 Consumes RawEvent from Pathfinder topic
        // 2.2 Assign Watermark
        // 2.3 Filter out large message
        // 2.4 RawEvent -> UbiEvent
        // 2.5 Union all UbiEvent

        // kafka data source
        KafkaSource<RawEvent> rnoPathfinderKafkaSource = getKafkaSource(flinkEnv, RNO, REGISTRY_URL);
        KafkaSource<RawEvent> lvsPathfinderKafkaSource = getKafkaSource(flinkEnv, LVS, REGISTRY_URL);
        KafkaSource<RawEvent> slcPathfinderKafkaSource = getKafkaSource(flinkEnv, SLC, REGISTRY_URL);

        WatermarkStrategy<RawEvent> watermarkStrategy =
                WatermarkStrategy.<RawEvent>forBoundedOutOfOrderness(Duration.ofMinutes(MAX_OUT_OF_ORDERNESS))
                                 .withIdleness(Duration.ofMinutes(IDLE_SOURCE_TIMEOUT))
                                 .withTimestampAssigner(new RawEventTimestampAssigner());

        SingleOutputStreamOperator<UbiEvent> rnoEventStream =
                executionEnvironment.fromSource(rnoPathfinderKafkaSource, watermarkStrategy, "Pathfinder RNO Source")
                                    .uid("source-kafka-pathfinder-rno")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_RNO)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .flatMap(new LargeMessageHandler(
                                            LARGE_MESSAGE_MAX_BYTES,
                                            SUB_URL_QUERY_STRING_LENGTH,
                                            TRUNCATE_URL_QUERY_STRING,
                                            LARGE_MSG_PAGEID_MONITOR))
                                    .name("Large Message Filter - RNO")
                                    .uid("large-message-filter-rno")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_RNO)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .map(new EventMapFunction(flinkEnv.getCjsConfigMap()))
                                    .name("Event Operator - RNO")
                                    .uid("event-operator-rno")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_RNO)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        SingleOutputStreamOperator<UbiEvent> lvsEventStream =
                executionEnvironment.fromSource(lvsPathfinderKafkaSource, watermarkStrategy, "Pathfinder LVS Source")
                                    .uid("source-kafka-pathfinder-lvs")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_LVS)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .flatMap(new LargeMessageHandler(
                                            LARGE_MESSAGE_MAX_BYTES,
                                            SUB_URL_QUERY_STRING_LENGTH,
                                            TRUNCATE_URL_QUERY_STRING,
                                            LARGE_MSG_PAGEID_MONITOR))
                                    .name("Large Message Filter - LVS")
                                    .uid("large-message-filter-lvs")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_LVS)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .map(new EventMapFunction(flinkEnv.getCjsConfigMap()))
                                    .name("Event Operator - LVS")
                                    .uid("event-operator-lvs")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_LVS)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        SingleOutputStreamOperator<UbiEvent> slcEventStream =
                executionEnvironment.fromSource(slcPathfinderKafkaSource, watermarkStrategy, "Pathfinder SLC Source")
                                    .uid("source-kafka-pathfinder-slc")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_SLC)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .flatMap(new LargeMessageHandler(
                                            LARGE_MESSAGE_MAX_BYTES,
                                            SUB_URL_QUERY_STRING_LENGTH,
                                            TRUNCATE_URL_QUERY_STRING,
                                            LARGE_MSG_PAGEID_MONITOR))
                                    .name("Large Message Filter - SLC")
                                    .uid("large-message-filter-slc")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_SLC)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .map(new EventMapFunction(flinkEnv.getCjsConfigMap()))
                                    .name("Event Operator - SLC")
                                    .uid("event-operator-slc")
                                    .slotSharingGroup(SLOT_GROUP_SOURCE_SLC)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        // union ubiEvent from SLC/RNO/LVS
        DataStream<UbiEvent> ubiEventDataStream = rnoEventStream.union(lvsEventStream).union(slcEventStream);

        // refine windowsoperator
        // 3. Session Operator
        // 3.1 Session window
        // 3.2 Session indicator accumulation
        // 3.3 Session Level bot detection (via bot rule & signature)
        // 3.4 Event level bot detection (via session flag)
        SingleOutputStreamOperator<UbiSession> ubiSessionDataStream =
                ubiEventDataStream.keyBy("guid")
                                  .window(SojEventTimeSessionWindows.withGapAndMaxDuration(minutes(30), hours(24)))
                                  .trigger(CompositeTrigger.Builder.create()
                                                                   .trigger(EventTimeTrigger.create())
                                                                   .trigger(MidnightOpenSessionTrigger.of(hours(7)))
                                                                   .build())
                                  .sideOutputLateData(OutputTagConstants.lateEventOutputTag)
                                  .aggregate(new UbiSessionAgg(), new UbiSessionWindowProcessFunction());

        WindowOperatorHelper.enrichWindowOperator(
                (OneInputTransformation) ubiSessionDataStream.getTransformation(),
                new UbiEventMapWithStateFunction(),
                OutputTagConstants.mappedEventOutputTag);

        ubiSessionDataStream.name("Session Operator")
                            .uid("session-operator")
                            .slotSharingGroup(SLOT_GROUP_SESSION)
                            .setParallelism(PARALLELISM_SESSION);


        SideOutputDataStream<UbiEvent> ubiEventWithSessionIdStream =
                ubiSessionDataStream.getSideOutput(OutputTagConstants.mappedEventOutputTag);

        SideOutputDataStream<UbiEvent> lateEventStream =
                ubiSessionDataStream.getSideOutput(OutputTagConstants.lateEventOutputTag);

        // ubiSession to SessionCore
        DataStream<SessionCore> sessionCoreDataStream =
                ubiSessionDataStream.filter(new OpenSessionFilterFunction())
                                    .name("UbiSession Open Filter")
                                    .uid("ubisession-open-filter")
                                    .slotSharingGroup(SLOT_GROUP_SESSION)
                                    .setParallelism(PARALLELISM_SESSION)
                                    .map(new UbiSessionToSessionCoreMapFunction())
                                    .name("UbiSession To SessionCore")
                                    .uid("ubisession-to-sessioncore")
                                    .slotSharingGroup(SLOT_GROUP_SESSION)
                                    .setParallelism(PARALLELISM_SESSION);

        // 4. Attribute Operator
        // 4.1 Sliding window
        // 4.2 Attribute indicator accumulation
        // 4.3 Attribute level bot detection (via bot rule)
        // 4.4 Store bot signature
        DataStream<AgentIpAttribute> agentIpAttributeDatastream =
                sessionCoreDataStream.keyBy("userAgent", "ip")
                                     .window(TumblingEventTimeWindows.of(minutes(5)))
                                     .aggregate(new AgentIpAttributeAgg(), new AgentIpWindowProcessFunction())
                                     .name("Attribute Operator (Agent+IP Pre-Aggregation)")
                                     .uid("attribute-operator-pre-aggregation")
                                     .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                     .setParallelism(PARALLELISM_AGENT_IP);

        DataStream<BotSignature> agentIpSignatureDataStream =
                agentIpAttributeDatastream.keyBy("agent", "clientIp")
                                          .window(SlidingEventTimeWindows.of(hours(24), hours(12), hours(7)))
                                          .trigger(OnElementEarlyFiringTrigger.create())
                                          .aggregate(new AgentIpAttributeAggSliding(),
                                                     new AgentIpSignatureWindowProcessFunction())
                                          .name("Attribute Operator (Agent+IP)")
                                          .uid("attribute-operator-agent-ip")
                                          .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                          .setParallelism(PARALLELISM_AGENT_IP);

        DataStream<BotSignature> agentSignatureDataStream =
                agentIpAttributeDatastream.keyBy("agent")
                                          .window(SlidingEventTimeWindows.of(hours(24), hours(12), hours(7)))
                                          .trigger(OnElementEarlyFiringTrigger.create())
                                          .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
                                          .name("Attribute Operator (Agent)")
                                          .uid("attribute-operator-agent")
                                          .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                          .setParallelism(PARALLELISM_AGENT_IP);

        DataStream<BotSignature> ipSignatureDataStream =
                agentIpAttributeDatastream.keyBy("clientIp")
                                          .window(SlidingEventTimeWindows.of(hours(24), hours(12), hours(7)))
                                          .trigger(OnElementEarlyFiringTrigger.create())
                                          .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
                                          .name("Attribute Operator (IP)")
                                          .uid("attribute-operator-ip")
                                          .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                          .setParallelism(PARALLELISM_AGENT_IP);

        // union attribute signature for broadcast
        DataStream<BotSignature> attributeSignatureDataStream = agentIpSignatureDataStream
                .union(agentSignatureDataStream)
                .union(ipSignatureDataStream);

        // attribute signature broadcast
        BroadcastStream<BotSignature> attributeSignatureBroadcastStream =
                attributeSignatureDataStream.broadcast(MapStateDesc.attributeSignatureDesc);

        // transform ubiEvent,ubiSession to same type and union
        DataStream<Either<UbiEvent, UbiSession>> ubiSessionTransDataStream =
                ubiSessionDataStream.map(new DetectableSessionMapFunction())
                                    .name("Transform UbiSession for Union")
                                    .uid("transform-ubisession-for-union")
                                    .slotSharingGroup(SLOT_GROUP_SESSION)
                                    .setParallelism(PARALLELISM_SESSION);

        DataStream<Either<UbiEvent, UbiSession>> ubiEventTransDataStream =
                ubiEventWithSessionIdStream.map(new DetectableEventMapFunction())
                                           .name("Transform UbiEvent for Union")
                                           .uid("transform-ubievent-for-union")
                                           .slotSharingGroup(SLOT_GROUP_SESSION)
                                           .setParallelism(PARALLELISM_SESSION);

        DataStream<Either<UbiEvent, UbiSession>> detectableDataStream =
                ubiSessionTransDataStream.union(ubiEventTransDataStream);

        // connect ubiEvent,ubiSession DataStream and broadcast Stream
        SingleOutputStreamOperator<UbiEvent> signatureBotDetectionForEvent =
                detectableDataStream.rescale()
                                    .connect(attributeSignatureBroadcastStream)
                                    .process(new AttributeBroadcastProcessFunctionForDetectable(
                                            OutputTagConstants.sessionOutputTag))
                                    .name("Signature Bot Detector")
                                    .uid("signature-bot-detector")
                                    .slotSharingGroup(SLOT_GROUP_SESSION)
                                    .setParallelism(PARALLELISM_BROADCAST);

        SideOutputDataStream<UbiSession> signatureBotDetectionForSession =
                signatureBotDetectionForEvent.getSideOutput(OutputTagConstants.sessionOutputTag);

        // ubiEvent to sojEvent
        SingleOutputStreamOperator<SojEvent> sojEventWithSessionId =
                signatureBotDetectionForEvent.process(new UbiEventToSojEventProcessFunction(
                                                     OutputTagConstants.botEventOutputTag))
                                             .name("UbiEvent to SojEvent")
                                             .uid("ubievent-to-sojevent")
                                             .slotSharingGroup(SLOT_GROUP_EVENT_SINK)
                                             .setParallelism(PARALLELISM_BROADCAST);

        DataStream<SojEvent> botSojEventStream = sojEventWithSessionId
                .getSideOutput(OutputTagConstants.botEventOutputTag);

        // ubiSession to sojSession
        SingleOutputStreamOperator<SojSession> sojSessionStream =
                signatureBotDetectionForSession.process(new UbiSessionToSojSessionProcessFunction(
                                                       OutputTagConstants.botSessionOutputTag))
                                               .name("UbiSession to SojSession")
                                               .uid("ubisession-to-sojsession")
                                               .slotSharingGroup(SLOT_GROUP_SESSION)
                                               .setParallelism(PARALLELISM_BROADCAST);

        DataStream<SojSession> botSojSessionStream =
                sojSessionStream.getSideOutput(OutputTagConstants.botSessionOutputTag);

        // extract sessionMetrics from ubiSession
        SingleOutputStreamOperator<SessionMetrics> sessionMetricsStream =
                signatureBotDetectionForSession.process(new UbiSessionToSessionMetricsProcessFunction())
                                               .name("UbiSession to SessionMetrics")
                                               .uid("ubisession-to-session-metrics")
                                               .slotSharingGroup(SLOT_GROUP_SESSION)
                                               .setParallelism(PARALLELISM_BROADCAST);

        // 5. Sink data to Kafka for downstream Realtime and Batch processing
        // 5.1 IP Signature
        // 5.2 Sessions (ended)
        // 5.3 Events (with session ID & bot flags)
        // 5.4 Events late

        // kafka sinks
        KafkaSink<SojSession> nonbotSojSessionKafkaSink = getKafkaSinkForSojSession(
                SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS, REGISTRY_URL, SUBJECT_SOJSESSION, TOPIC_NONBOT_SESSION);

        KafkaSink<SojSession> botSojSessionKafkaSink = getKafkaSinkForSojSession(
                SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS, REGISTRY_URL, SUBJECT_SOJSESSION, TOPIC_BOT_SESSION);

        KafkaSink<SojEvent> nonbotSojEventKafkaSink = getKafkaSinkForSojEvent(
                SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS, REGISTRY_URL, SUBJECT_SOJEVENT, TOPIC_NONBOT_EVENT);

        KafkaSink<SojEvent> botSojEventKafkaSink = getKafkaSinkForSojEvent(
                SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS, REGISTRY_URL, SUBJECT_SOJEVENT, TOPIC_BOT_EVENT);


        // kafka sink for bot and nonbot sojsession
        sojSessionStream.sinkTo(nonbotSojSessionKafkaSink)
                        .name("Kafka Sink: SojSession Non-Bot")
                        .uid("nonbot-sojsession-sink")
                        .slotSharingGroup(SLOT_GROUP_SESSION)
                        .setParallelism(PARALLELISM_BROADCAST);

        botSojSessionStream.sinkTo(botSojSessionKafkaSink)
                           .name("Kafka Sink: SojSession Bot")
                           .uid("bot-sojsession-sink")
                           .slotSharingGroup(SLOT_GROUP_SESSION)
                           .setParallelism(PARALLELISM_BROADCAST);


        // kafka sink for bot and nonbot sojevent
        sojEventWithSessionId.sinkTo(nonbotSojEventKafkaSink)
                             .name("Kafka Sink: SojEvent Non-Bot")
                             .uid("nonbot-sojevent-sink")
                             .slotSharingGroup(SLOT_GROUP_EVENT_SINK)
                             .setParallelism(PARALLELISM_BROADCAST);

        botSojEventStream.sinkTo(botSojEventKafkaSink)
                         .name("Kafka Sink: SojEvent Bot")
                         .uid("bot-sojevent-sink")
                         .slotSharingGroup(SLOT_GROUP_EVENT_SINK)
                         .setParallelism(PARALLELISM_BROADCAST);

        // kafka sink for SessionMetrics
        sessionMetricsStream.sinkTo(getKafkaSinkForSessionMetrics(SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS,
                                                                  REGISTRY_URL, SUBJECT_SESSION_METRICS,
                                                                  TOPIC_SESSION_METRICS))
                            .name("Kafka Sink: Session Metrics")
                            .uid("bot-metrics-sink-kafka")
                            .slotSharingGroup(SLOT_GROUP_SESSION)
                            .setParallelism(PARALLELISM_BROADCAST);

        // metrics collector for end to end
        signatureBotDetectionForEvent.process(new RTPipelineMetricsCollectorProcessFunction(metricWindow))
                                     .name("Pipeline Metrics Collector")
                                     .uid("pipeline-metrics-collector")
                                     .slotSharingGroup(SLOT_GROUP_SESSION)
                                     .setParallelism(PARALLELISM_BROADCAST);

        // metrics collector for signature generation or expiration
        agentIpSignatureDataStream.process(new AgentIpMetricsCollectorProcessFunction())
                                  .name("AgentIp Metrics Collector")
                                  .uid("agent-ip-metrics-collector")
                                  .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                  .setParallelism(PARALLELISM_AGENT_IP);

        agentSignatureDataStream.process(new AgentMetricsCollectorProcessFunction())
                                .name("Agent Metrics Collector")
                                .uid("agent-metrics-id")
                                .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                .setParallelism(PARALLELISM_AGENT_IP);

        ipSignatureDataStream.process(new IpMetricsCollectorProcessFunction())
                             .name("Ip Metrics Collector")
                             .uid("ip-metrics-id")
                             .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                             .setParallelism(PARALLELISM_AGENT_IP);

        // signature sink
        agentIpSignatureDataStream.sinkTo(getKafkaSinkForBotSignature(SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS,
                                                                      TOPIC_BOT_SIG_AGENT_IP, "userAgent"))
                                  .name(String.format("%s Signature", Constants.AGENTIP))
                                  .uid(String.format("signature-%s-sink", Constants.AGENTIP))
                                  .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                  .setParallelism(PARALLELISM_AGENT_IP);

        agentSignatureDataStream.sinkTo(getKafkaSinkForBotSignature(SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS,
                                                                    TOPIC_BOT_SIG_AGENT, "userAgent"))
                                .name(String.format("%s Signature", Constants.AGENT))
                                .uid(String.format("signature-%s-sink", Constants.AGENT))
                                .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                                .setParallelism(PARALLELISM_AGENT_IP);

        ipSignatureDataStream.sinkTo(getKafkaSinkForBotSignature(SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS,
                                                                 TOPIC_BOT_SIG_IP, "ip"))
                             .name(String.format("%s Signature", Constants.IP))
                             .uid(String.format("signature-%s-sink", Constants.IP))
                             .slotSharingGroup(SLOT_GROUP_CROSS_SESSION)
                             .setParallelism(PARALLELISM_AGENT_IP);

        // kafka sink for late event
        DataStream<SojEvent> lateSojEventStream =
                lateEventStream.map(new UbiEventToSojEventMapFunction())
                               .name("Late UbiEvent to SojEvent")
                               .uid("late-ubievent-to-sojevent")
                               .slotSharingGroup(SLOT_GROUP_SESSION)
                               .setParallelism(PARALLELISM_SESSION);

        lateSojEventStream.sinkTo(getKafkaSinkForSojEvent(SINK_KAFKA_BROKERS, KAFKA_PRODUCER_PROPS, REGISTRY_URL,
                                                          SUBJECT_SOJEVENT, TOPIC_LATE_EVENT))
                          .name("Kafka Sink: SojEvent Late")
                          .uid("late-sojevent-sink")
                          .slotSharingGroup(SLOT_GROUP_SESSION)
                          .setParallelism(PARALLELISM_SESSION);

        // Submit this job
        flinkEnv.execute(executionEnvironment);
    }

    private static KafkaSource<RawEvent> getKafkaSource(FlinkEnv flinkEnv, DataCenter dc, String schemaRegistryUrl) {

        return KafkaSource.<RawEvent>builder()
                          .setBootstrapServers(flinkEnv.getSourceKafkaBrokersOfDC(dc))
                          .setGroupId(flinkEnv.getSourceKafkaGroupId())
                          .setTopics(flinkEnv.getSourceKafkaTopics())
                          .setProperties(flinkEnv.getKafkaConsumerProps())
                          .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                          .setDeserializer(KafkaRecordDeserializationSchema.of(
                                  new RawEventKafkaDeserializationSchemaWrapper(
                                          Sets.newHashSet(),
                                          new RawEventDeserializationSchema(schemaRegistryUrl)
                                  )
                          ))
                          .build();
    }

    private static KafkaSink<SojEvent> getKafkaSinkForSojEvent(String brokers, Properties producerConfigs,
                                                               String schemaRegistryUrl, String subjectName,
                                                               String topic) {
        Preconditions.checkNotNull(brokers);
        Preconditions.checkNotNull(producerConfigs);
        Preconditions.checkNotNull(schemaRegistryUrl);
        Preconditions.checkNotNull(subjectName);
        Preconditions.checkNotNull(topic);

        // kafka sink
        return KafkaSink.<SojEvent>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(producerConfigs)
                        .setRecordSerializer(new SojEventKafkaRecordSerializationSchema(
                                schemaRegistryUrl, subjectName, topic))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
    }

    private static KafkaSink<SojSession> getKafkaSinkForSojSession(String brokers, Properties producerConfigs,
                                                                   String schemaRegistryUrl, String subjectName,
                                                                   String topic) {
        Preconditions.checkNotNull(brokers);
        Preconditions.checkNotNull(producerConfigs);
        Preconditions.checkNotNull(schemaRegistryUrl);
        Preconditions.checkNotNull(subjectName);
        Preconditions.checkNotNull(topic);

        // sink to kafka
        return KafkaSink.<SojSession>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(producerConfigs)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema
                                        .<SojSession>builder()
                                        .setTopic(topic)
                                        .setKeySerializationSchema(new SojSessionKeySerializerSchema())
                                        .setValueSerializationSchema(new SojSessionValueSerializerSchema(
                                                schemaRegistryUrl, subjectName))
                                        .build()
                        )
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
    }

    private static KafkaSink<SessionMetrics> getKafkaSinkForSessionMetrics(String brokers, Properties producerConfigs,
                                                                           String schemaRegistryUrl, String subjectName,
                                                                           String topic) {
        Preconditions.checkNotNull(brokers);
        Preconditions.checkNotNull(producerConfigs);
        Preconditions.checkNotNull(schemaRegistryUrl);
        Preconditions.checkNotNull(subjectName);
        Preconditions.checkNotNull(topic);

        // sink to kafka
        return KafkaSink.<SessionMetrics>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(producerConfigs)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema
                                        .<SessionMetrics>builder()
                                        .setTopic(topic)
                                        .setKeySerializationSchema(new SessionMetricsKeySerializerSchema())
                                        .setValueSerializationSchema(new SessionMetricsValueSerializerSchema(
                                                schemaRegistryUrl, subjectName))
                                        .build()
                        )
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
    }

    private static KafkaSink<BotSignature> getKafkaSinkForBotSignature(String brokers, Properties producerConfigs,
                                                                       String topic, String keyField) {
        Preconditions.checkNotNull(brokers);
        Preconditions.checkNotNull(producerConfigs);
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(keyField);

        // sink to kafka
        return KafkaSink.<BotSignature>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(producerConfigs)
                        .setRecordSerializer(new BotSignatureKafkaRecordSerializationSchema(topic, keyField))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
    }

}
