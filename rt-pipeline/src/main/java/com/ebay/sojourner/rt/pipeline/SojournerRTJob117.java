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

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_WATERMARK_IDLE_SOURCE_TIMEOUT_IN_MIN;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_WATERMARK_MAX_OUT_OF_ORDERNESS_IN_MIN;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_REGISTRY_URL;
import static com.ebay.sojourner.flink.common.DataCenter.LVS;
import static com.ebay.sojourner.flink.common.DataCenter.RNO;
import static com.ebay.sojourner.flink.common.DataCenter.SLC;
import static org.apache.flink.streaming.api.windowing.time.Time.hours;
import static org.apache.flink.streaming.api.windowing.time.Time.minutes;

public class SojournerRTJob117 {

    public static void main(String[] args) throws Exception {
        // 1. Prepare Flink environment
        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_DATA_SOURCE_RNO = "kafka-data-source-rno";
        final String UID_KAFKA_DATA_SOURCE_LVS = "kafka-data-source-lvs";
        final String UID_KAFKA_DATA_SOURCE_SLC = "kafka-data-source-slc";

        // operator name
        final String NAME_KAFKA_DATA_SOURCE_RNO = String.format("Kafka: %s - RNO", flinkEnv.getSourceKafkaStreamName());
        final String NAME_KAFKA_DATA_SOURCE_LVS = String.format("Kafka: %s - LVS", flinkEnv.getSourceKafkaStreamName());
        final String NAME_KAFKA_DATA_SOURCE_SLC = String.format("Kafka: %s - SLC", flinkEnv.getSourceKafkaStreamName());

        // config
        final Long LARGE_MESSAGE_MAX_BYTES = flinkEnv.getLong("flink.app.filter.large-message.max-bytes");
        final Integer SUB_URL_QUERY_STRING_LENGTH =
                flinkEnv.getInteger("flink.app.filter.large-message.sub-url-query-string-length");
        final Boolean TRUNCATE_URL_QUERY_STRING =
                flinkEnv.getBoolean("flink.app.filter.large-message.truncate-url-query-string");

        final int sessionParallelism = flinkEnv.getInteger("flink.app.parallelism.session");
        final int broadcastParallelism = flinkEnv.getInteger("flink.app.parallelism.broadcast");
        final int agentIpParallelism = flinkEnv.getInteger("flink.app.parallelism.agent-ip");

        final String sourceRNOSlotGroup = "source-rno";
        final String sourceLVSSlotGroup = "source-lvs";
        final String sourceSLCSlotGroup = "source-slc";
        final String sessionSlotGroup = "session";
        final String crossSessionSlotGroup = "cross-session";

        final String registryUrl = flinkEnv.getString(RHEOS_REGISTRY_URL);

        // event topics
        final String botEventTopic = flinkEnv.getString("flink.app.sink.kafka.topic.event.bot");
        final String nonbotEventTopic = flinkEnv.getString("flink.app.sink.kafka.topic.event.non-bot");
        final String lateEventTopic = flinkEnv.getString("flink.app.sink.kafka.topic.event.late");
        // session topics
        final String botSessionTopic = flinkEnv.getString("flink.app.sink.kafka.topic.session.bot");
        final String nonbotSessionTopic = flinkEnv.getString("flink.app.sink.kafka.topic.session.non-bot");
        final String sessionMetricsTopic = flinkEnv.getString("flink.app.sink.kafka.topic.session.metrics");
        // bot signature topics
        final String agentIpSignatureTopic =
                flinkEnv.getString("flink.app.sink.kafka.topic.bot-signature.agent-ip");
        final String agentSignatureTopic = flinkEnv.getString("flink.app.sink.kafka.topic.bot-signature.agent");
        final String ipSignatureTopic = flinkEnv.getString("flink.app.sink.kafka.topic.bot-signature.ip");

        // schema subject names
        final String sojeventSubject = flinkEnv.getString("flink.app.sink.kafka.subject.event");
        final String sojsessionSubject = flinkEnv.getString("flink.app.sink.kafka.subject.session");
        final String sessionMetricsSubject = flinkEnv.getString("flink.app.sink.kafka.subject.session-metrics");

        final String sinkKafkaBrokers = flinkEnv.getSinkKafkaBrokers();
        final Properties kafkaProducerProps = flinkEnv.getKafkaProducerProps();

        final int metricWindow = 70000;

        // 2. Source & Filter & Event
        // 2.1 Consumes RawEvent from Pathfinder topic
        // 2.2 Assign Watermark
        // 2.3 Filter out large message
        // 2.4 RawEvent -> UbiEvent
        // 2.5 Union all UbiEvent

        // kafka data source
        KafkaSource<RawEvent> rnoPathfinderKafkaSource = getKafkaSource(flinkEnv, RNO);
        KafkaSource<RawEvent> lvsPathfinderKafkaSource = getKafkaSource(flinkEnv, LVS);
        KafkaSource<RawEvent> slcPathfinderKafkaSource = getKafkaSource(flinkEnv, SLC);


        final Integer outOfOrderness = flinkEnv.getInteger(FLINK_APP_WATERMARK_MAX_OUT_OF_ORDERNESS_IN_MIN);
        final Integer idleness = flinkEnv.getInteger(FLINK_APP_WATERMARK_IDLE_SOURCE_TIMEOUT_IN_MIN);

        WatermarkStrategy<RawEvent> watermarkStrategy =
                WatermarkStrategy.<RawEvent>forBoundedOutOfOrderness(Duration.ofMinutes(outOfOrderness))
                                 .withIdleness(Duration.ofMinutes(idleness))
                                 .withTimestampAssigner(new RawEventTimestampAssigner());

        SingleOutputStreamOperator<UbiEvent> rnoEventStream =
                executionEnvironment.fromSource(rnoPathfinderKafkaSource, watermarkStrategy, NAME_KAFKA_DATA_SOURCE_RNO)
                                    .uid(UID_KAFKA_DATA_SOURCE_RNO)
                                    .slotSharingGroup(sourceRNOSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .flatMap(new LargeMessageHandler(
                                            LARGE_MESSAGE_MAX_BYTES,
                                            SUB_URL_QUERY_STRING_LENGTH,
                                            TRUNCATE_URL_QUERY_STRING))
                                    .name("Large Message Filter - RNO")
                                    .uid("large-message-filter-rno")
                                    .slotSharingGroup(sourceRNOSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .map(new EventMapFunction())
                                    .name("Event Operator - RNO")
                                    .uid("event-operator-rno")
                                    .slotSharingGroup(sourceRNOSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        SingleOutputStreamOperator<UbiEvent> lvsEventStream =
                executionEnvironment.fromSource(lvsPathfinderKafkaSource, watermarkStrategy, NAME_KAFKA_DATA_SOURCE_LVS)
                                    .uid(UID_KAFKA_DATA_SOURCE_LVS)
                                    .slotSharingGroup(sourceLVSSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .flatMap(new LargeMessageHandler(
                                            LARGE_MESSAGE_MAX_BYTES,
                                            SUB_URL_QUERY_STRING_LENGTH,
                                            TRUNCATE_URL_QUERY_STRING))
                                    .name("Large Message Filter - LVS")
                                    .uid("large-message-filter-lvs")
                                    .slotSharingGroup(sourceLVSSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .map(new EventMapFunction())
                                    .name("Event Operator - LVS")
                                    .uid("event-operator-lvs")
                                    .slotSharingGroup(sourceLVSSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        SingleOutputStreamOperator<UbiEvent> slcEventStream =
                executionEnvironment.fromSource(slcPathfinderKafkaSource, watermarkStrategy, NAME_KAFKA_DATA_SOURCE_SLC)
                                    .uid(UID_KAFKA_DATA_SOURCE_SLC)
                                    .slotSharingGroup(sourceSLCSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .flatMap(new LargeMessageHandler(
                                            LARGE_MESSAGE_MAX_BYTES,
                                            SUB_URL_QUERY_STRING_LENGTH,
                                            TRUNCATE_URL_QUERY_STRING))
                                    .name("Large Message Filter - SLC")
                                    .uid("large-message-filter-slc")
                                    .slotSharingGroup(sourceSLCSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism())
                                    .map(new EventMapFunction())
                                    .name("Event Operator - SLC")
                                    .uid("event-operator-slc")
                                    .slotSharingGroup(sourceSLCSlotGroup)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        // union ubiEvent from SLC/RNO/LVS
        DataStream<UbiEvent> ubiEventDataStream = rnoEventStream.union(lvsEventStream)
                                                                .union(slcEventStream);

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
                            .slotSharingGroup(sessionSlotGroup)
                            .setParallelism(sessionParallelism);


        SideOutputDataStream<UbiEvent> ubiEventWithSessionIdStream =
                ubiSessionDataStream.getSideOutput(OutputTagConstants.mappedEventOutputTag);

        SideOutputDataStream<UbiEvent> lateEventStream =
                ubiSessionDataStream.getSideOutput(OutputTagConstants.lateEventOutputTag);

        // ubiSession to SessionCore
        DataStream<SessionCore> sessionCoreDataStream =
                ubiSessionDataStream.filter(new OpenSessionFilterFunction())
                                    .name("UbiSession Open Filter")
                                    .uid("ubisession-open-filter")
                                    .slotSharingGroup(sessionSlotGroup)
                                    .setParallelism(sessionParallelism)
                                    .map(new UbiSessionToSessionCoreMapFunction())
                                    .name("UbiSession To SessionCore")
                                    .uid("ubisession-to-sessioncore")
                                    .slotSharingGroup(sessionSlotGroup)
                                    .setParallelism(sessionParallelism);

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
                                     .slotSharingGroup(crossSessionSlotGroup)
                                     .setParallelism(agentIpParallelism);

        DataStream<BotSignature> agentIpSignatureDataStream =
                agentIpAttributeDatastream.keyBy("agent", "clientIp")
                                          .window(SlidingEventTimeWindows.of(hours(24), hours(12), hours(7)))
                                          .trigger(OnElementEarlyFiringTrigger.create())
                                          .aggregate(new AgentIpAttributeAggSliding(),
                                                     new AgentIpSignatureWindowProcessFunction())
                                          .name("Attribute Operator (Agent+IP)")
                                          .uid("attribute-operator-agent-ip")
                                          .slotSharingGroup(crossSessionSlotGroup)
                                          .setParallelism(agentIpParallelism);

        DataStream<BotSignature> agentSignatureDataStream =
                agentIpAttributeDatastream.keyBy("agent")
                                          .window(SlidingEventTimeWindows.of(hours(24), hours(12), hours(7)))
                                          .trigger(OnElementEarlyFiringTrigger.create())
                                          .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
                                          .name("Attribute Operator (Agent)")
                                          .uid("attribute-operator-agent")
                                          .slotSharingGroup(crossSessionSlotGroup)
                                          .setParallelism(agentIpParallelism);

        DataStream<BotSignature> ipSignatureDataStream =
                agentIpAttributeDatastream.keyBy("clientIp")
                                          .window(SlidingEventTimeWindows.of(hours(24), hours(12), hours(7)))
                                          .trigger(OnElementEarlyFiringTrigger.create())
                                          .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
                                          .name("Attribute Operator (IP)")
                                          .uid("attribute-operator-ip")
                                          .slotSharingGroup(crossSessionSlotGroup)
                                          .setParallelism(agentIpParallelism);

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
                                    .slotSharingGroup(sessionSlotGroup)
                                    .setParallelism(sessionParallelism);

        DataStream<Either<UbiEvent, UbiSession>> ubiEventTransDataStream =
                ubiEventWithSessionIdStream.map(new DetectableEventMapFunction())
                                           .name("Transform UbiEvent for Union")
                                           .uid("transform-ubievent-for-union")
                                           .slotSharingGroup(sessionSlotGroup)
                                           .setParallelism(sessionParallelism);

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
                                    .slotSharingGroup(sessionSlotGroup)
                                    .setParallelism(broadcastParallelism);

        SideOutputDataStream<UbiSession> signatureBotDetectionForSession =
                signatureBotDetectionForEvent.getSideOutput(OutputTagConstants.sessionOutputTag);

        // ubiEvent to sojEvent
        SingleOutputStreamOperator<SojEvent> sojEventWithSessionId =
                signatureBotDetectionForEvent.process(new UbiEventToSojEventProcessFunction(
                                                     OutputTagConstants.botEventOutputTag))
                                             .name("UbiEvent to SojEvent")
                                             .uid("ubievent-to-sojevent")
                                             .slotSharingGroup(sessionSlotGroup)
                                             .setParallelism(broadcastParallelism);

        DataStream<SojEvent> botSojEventStream = sojEventWithSessionId
                .getSideOutput(OutputTagConstants.botEventOutputTag);

        // ubiSession to sojSession
        SingleOutputStreamOperator<SojSession> sojSessionStream =
                signatureBotDetectionForSession.process(new UbiSessionToSojSessionProcessFunction(
                                                       OutputTagConstants.botSessionOutputTag))
                                               .name("UbiSession to SojSession")
                                               .uid("ubisession-to-sojsession")
                                               .slotSharingGroup(sessionSlotGroup)
                                               .setParallelism(broadcastParallelism);

        DataStream<SojSession> botSojSessionStream =
                sojSessionStream.getSideOutput(OutputTagConstants.botSessionOutputTag);

        // extract sessionMetrics from ubiSession
        SingleOutputStreamOperator<SessionMetrics> sessionMetricsStream =
                signatureBotDetectionForSession.process(new UbiSessionToSessionMetricsProcessFunction())
                                               .name("UbiSession to SessionMetrics")
                                               .uid("ubisession-to-session-metrics")
                                               .slotSharingGroup(sessionSlotGroup)
                                               .setParallelism(broadcastParallelism);

        // 5. Load data to file system for batch processing
        // 5.1 IP Signature
        // 5.2 Sessions (ended)
        // 5.3 Events (with session ID & bot flags)
        // 5.4 Events late

        // kafka sinks
        KafkaSink<SojSession> sojSessionKafkaSink = getKafkaSinkForSojSession(sinkKafkaBrokers, kafkaProducerProps,
                                                                              registryUrl, nonbotSessionTopic,
                                                                              sojsessionSubject);

        KafkaSink<SojSession> botSojSessionKafkaSink = getKafkaSinkForSojSession(sinkKafkaBrokers, kafkaProducerProps,
                                                                                 registryUrl, botSessionTopic,
                                                                                 sojsessionSubject);

        KafkaSink<SojEvent> sojEventKafkaSink = getKafkaSinkForSojEvent(sinkKafkaBrokers, kafkaProducerProps,
                                                                        registryUrl, nonbotEventTopic,
                                                                        sojeventSubject);

        KafkaSink<SojEvent> botSojEventKafkaSink = getKafkaSinkForSojEvent(sinkKafkaBrokers, kafkaProducerProps,
                                                                           registryUrl, botEventTopic,
                                                                           sojeventSubject);


        // kafka sink for bot and nonbot sojsession
        sojSessionStream.sinkTo(sojSessionKafkaSink)
                        .name("Kafka Sink: SojSession Non-Bot")
                        .uid("nonbot-sojsession-sink")
                        .slotSharingGroup(sessionSlotGroup)
                        .setParallelism(broadcastParallelism);

        botSojSessionStream.sinkTo(botSojSessionKafkaSink)
                           .name("Kafka Sink: SojSession Bot")
                           .uid("bot-sojsession-sink")
                           .slotSharingGroup(sessionSlotGroup)
                           .setParallelism(broadcastParallelism);


        // kafka sink for bot and nonbot sojevent
        sojEventWithSessionId.sinkTo(sojEventKafkaSink)
                             .name("Kafka Sink: SojEvent Non-Bot")
                             .uid("nonbot-sojevent-sink")
                             .slotSharingGroup(sessionSlotGroup)
                             .setParallelism(broadcastParallelism);

        botSojEventStream.sinkTo(botSojEventKafkaSink)
                         .name("Kafka Sink: SojEvent Bot")
                         .uid("bot-sojevent-sink")
                         .slotSharingGroup(sessionSlotGroup)
                         .setParallelism(broadcastParallelism);

        // kafka sink for SessionMetrics
        sessionMetricsStream.sinkTo(getKafkaSinkForSessionMetrics(sinkKafkaBrokers, kafkaProducerProps, registryUrl,
                                                                  sessionMetricsTopic, sessionMetricsSubject))
                            .name("Kafka Sink: Session Metrics")
                            .uid("bot-metrics-sink-kafka")
                            .slotSharingGroup(sessionSlotGroup)
                            .setParallelism(broadcastParallelism);

        // metrics collector for end to end
        signatureBotDetectionForEvent.process(new RTPipelineMetricsCollectorProcessFunction(metricWindow))
                                     .name("Pipeline Metrics Collector")
                                     .uid("pipeline-metrics-collector")
                                     .slotSharingGroup(sessionSlotGroup)
                                     .setParallelism(broadcastParallelism);

        // metrics collector for signature generation or expiration
        agentIpSignatureDataStream.process(new AgentIpMetricsCollectorProcessFunction())
                                  .name("AgentIp Metrics Collector")
                                  .uid("agent-ip-metrics-collector")
                                  .slotSharingGroup(crossSessionSlotGroup)
                                  .setParallelism(agentIpParallelism);

        agentSignatureDataStream.process(new AgentMetricsCollectorProcessFunction())
                                .name("Agent Metrics Collector")
                                .uid("agent-metrics-id")
                                .slotSharingGroup(crossSessionSlotGroup)
                                .setParallelism(agentIpParallelism);

        ipSignatureDataStream.process(new IpMetricsCollectorProcessFunction())
                             .name("Ip Metrics Collector")
                             .uid("ip-metrics-id")
                             .slotSharingGroup(crossSessionSlotGroup)
                             .setParallelism(agentIpParallelism);

        // signature sink
        agentIpSignatureDataStream.sinkTo(getKafkaSinkForBotSignature(sinkKafkaBrokers, kafkaProducerProps,
                                                                      agentIpSignatureTopic, "userAgent"))
                                  .name(String.format("%s Signature", Constants.AGENTIP))
                                  .uid(String.format("signature-%s-sink", Constants.AGENTIP))
                                  .slotSharingGroup(crossSessionSlotGroup)
                                  .setParallelism(agentIpParallelism);

        agentSignatureDataStream.sinkTo(getKafkaSinkForBotSignature(sinkKafkaBrokers, kafkaProducerProps,
                                                                    agentSignatureTopic, "userAgent"))
                                .name(String.format("%s Signature", Constants.AGENT))
                                .uid(String.format("signature-%s-sink", Constants.AGENT))
                                .slotSharingGroup(crossSessionSlotGroup)
                                .setParallelism(agentIpParallelism);

        ipSignatureDataStream.sinkTo(getKafkaSinkForBotSignature(sinkKafkaBrokers, kafkaProducerProps,
                                                                 ipSignatureTopic, "ip"))
                             .name(String.format("%s Signature", Constants.IP))
                             .uid(String.format("signature-%s-sink", Constants.IP))
                             .slotSharingGroup(crossSessionSlotGroup)
                             .setParallelism(agentIpParallelism);

        // kafka sink for late event
        DataStream<SojEvent> lateSojEventStream =
                lateEventStream.map(new UbiEventToSojEventMapFunction())
                               .name("Late UbiEvent to SojEvent")
                               .uid("late-ubievent-to-sojevent")
                               .slotSharingGroup(sessionSlotGroup)
                               .setParallelism(sessionParallelism);

        lateSojEventStream.sinkTo(getKafkaSinkForSojEvent(sinkKafkaBrokers, kafkaProducerProps,
                                                          registryUrl, lateEventTopic,
                                                          sojeventSubject))
                          .name("Kafka Sink: SojEvent Late")
                          .uid("late-sojevent-sink")
                          .slotSharingGroup(sessionSlotGroup)
                          .setParallelism(sessionParallelism);

        // Submit this job
        flinkEnv.execute(executionEnvironment);
    }

    private static KafkaSource<RawEvent> getKafkaSource(FlinkEnv flinkEnv, DataCenter dc) {
        final String REGISTRY_URL = flinkEnv.getString(RHEOS_REGISTRY_URL);

        return KafkaSource.<RawEvent>builder()
                          .setBootstrapServers(flinkEnv.getSourceKafkaBrokersOfDC(dc))
                          .setGroupId(flinkEnv.getSourceKafkaGroupId())
                          .setTopics(flinkEnv.getSourceKafkaTopics())
                          .setProperties(flinkEnv.getKafkaConsumerProps())
                          .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                          .setDeserializer(KafkaRecordDeserializationSchema.of(
                                  new RawEventKafkaDeserializationSchemaWrapper(
                                          Sets.newHashSet(),
                                          new RawEventDeserializationSchema(REGISTRY_URL)
                                  )
                          ))
                          .build();
    }

    private static KafkaSink<SojEvent> getKafkaSinkForSojEvent(String brokers, Properties producerConfigs,
                                                               String schemaRegistryUrl, String topic,
                                                               String subjectName) {
        Preconditions.checkNotNull(brokers);
        Preconditions.checkNotNull(producerConfigs);
        Preconditions.checkNotNull(schemaRegistryUrl);
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(subjectName);

        // kafka sink
        return KafkaSink.<SojEvent>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(producerConfigs)
                        .setRecordSerializer(new SojEventKafkaRecordSerializationSchema(schemaRegistryUrl,
                                                                                        subjectName, topic))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
    }

    private static KafkaSink<SojSession> getKafkaSinkForSojSession(String brokers, Properties producerConfigs,
                                                                   String schemaRegistryUrl, String topic,
                                                                   String subjectName) {
        Preconditions.checkNotNull(brokers);
        Preconditions.checkNotNull(producerConfigs);
        Preconditions.checkNotNull(schemaRegistryUrl);
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(subjectName);

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
                                                                           String schemaRegistryUrl, String topic,
                                                                           String subjectName) {
        Preconditions.checkNotNull(brokers);
        Preconditions.checkNotNull(producerConfigs);
        Preconditions.checkNotNull(schemaRegistryUrl);
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(subjectName);

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

        // sink to kafka
        return KafkaSink.<BotSignature>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(producerConfigs)
                        .setRecordSerializer(new BotSignatureKafkaRecordSerializationSchema(topic, keyField))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
    }

}
