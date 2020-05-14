package com.ebay.sojourner.ubd.rt.pipeline;

import com.ebay.sojourner.ubd.common.model.IntermediateSession;
import com.ebay.sojourner.ubd.common.model.RawEvent;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.model.UbiSession;
import com.ebay.sojourner.ubd.rt.common.metrics.EventMetricsCollectorProcessFunction;
import com.ebay.sojourner.ubd.rt.common.metrics.PipelineMetricsCollectorProcessFunction;
import com.ebay.sojourner.ubd.rt.connectors.kafka.KafkaConnectorFactory;
import com.ebay.sojourner.ubd.rt.connectors.kafka.KafkaSourceFunction;
import com.ebay.sojourner.ubd.rt.operators.event.EventMapFunction;
import com.ebay.sojourner.ubd.rt.operators.event.UbiEventMapWithStateFunction;
import com.ebay.sojourner.ubd.rt.operators.session.UbiSessionAgg;
import com.ebay.sojourner.ubd.rt.operators.session.UbiSessionToIntermediateSessionMapFunction;
import com.ebay.sojourner.ubd.rt.operators.session.UbiSessionWindowProcessFunction;
import com.ebay.sojourner.ubd.rt.util.AppEnv;
import com.ebay.sojourner.ubd.rt.util.Constants;
import com.ebay.sojourner.ubd.rt.util.ExecutionEnvUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorHelper;
import org.apache.flink.util.OutputTag;

public class SojournerUBDRTJob {

  public static void main(String[] args) throws Exception {
    // Make sure this is being executed at start up.
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    AppEnv.config(parameterTool);

    // 0.0 Prepare execution environment
    // 0.1 UBI configuration
    // 0.2 Flink configuration
    final StreamExecutionEnvironment executionEnvironment =
        ExecutionEnvUtil.prepare(parameterTool);

    // for soj nrt output
    // 1. Rheos Consumer
    // 1.1 Consume RawEvent from Rheos PathFinder topic
    // 1.2 Assign timestamps and emit watermarks.
    DataStream<RawEvent> rawEventDataStreamForRNO =
        executionEnvironment
            .addSource(KafkaSourceFunction.buildSource(Constants.TOPIC_PATHFINDER_EVENTS,
                Constants.BOOTSTRAP_SERVERS_RNO, Constants.GROUP_ID_RNO, RawEvent.class))
            .setParallelism(
                AppEnv.config().getFlink().getApp().getSourceParallelism() == null
                    ? 100
                    : AppEnv.config().getFlink().getApp().getSourceParallelism())
            .name("Rheos Kafka Consumer For RNO")
            .slotSharingGroup("RNO")
            .uid("kafkaSourceForRNO");

    DataStream<RawEvent> rawEventDataStreamForSLC =
        executionEnvironment
            .addSource(KafkaSourceFunction.buildSource(Constants.TOPIC_PATHFINDER_EVENTS,
                Constants.BOOTSTRAP_SERVERS_SLC, Constants.GROUP_ID_SLC, RawEvent.class))
            .setParallelism(
                AppEnv.config().getFlink().getApp().getSourceParallelism() == null
                    ? 100
                    : AppEnv.config().getFlink().getApp().getSourceParallelism())
            .name("Rheos Kafka Consumer For SLC")
            .slotSharingGroup("SLC")
            .uid("kafkaSourceForSLC");

    DataStream<RawEvent> rawEventDataStreamForLVS =
        executionEnvironment
            .addSource(KafkaSourceFunction.buildSource(Constants.TOPIC_PATHFINDER_EVENTS,
                Constants.BOOTSTRAP_SERVERS_LVS, Constants.GROUP_ID_LVS, RawEvent.class))
            .setParallelism(
                AppEnv.config().getFlink().getApp().getSourceParallelism() == null
                    ? 100
                    : AppEnv.config().getFlink().getApp().getSourceParallelism())
            .slotSharingGroup("LVS")
            .name("Rheos Kafka Consumer For LVS")
            .uid("kafkaSourceForLVS");

    // filter 33% throughput group by guid for reduce kafka consumer lag
    /*
    DataStream<RawEvent> filteredRawEvent = rawEventDataStream
        .filter(new RawEventFilterFunction())
        .name("RawEvent Filter Operator")
        .disableChaining()
        .uid("filterSource");
        */

    // 2. Event Operator
    // 2.1 Parse and transform RawEvent to UbiEvent
    // 2.2 Event level bot detection via bot rule
    DataStream<UbiEvent> ubiEventDataStreamForRNO = rawEventDataStreamForRNO
        .map(new EventMapFunction())
        .setParallelism(AppEnv.config().getFlink().getApp().getEventParallelism())
        .name("Event Operator For RNO")
        .slotSharingGroup("RNO")
        .uid("eventLevelForRNO");

    DataStream<UbiEvent> ubiEventDataStreamForLVS = rawEventDataStreamForLVS
        .map(new EventMapFunction())
        .setParallelism(AppEnv.config().getFlink().getApp().getEventParallelism())
        .name("Event Operator For LVS")
        .slotSharingGroup("LVS")
        .uid("eventLevelForLVS");

    DataStream<UbiEvent> ubiEventDataStreamForSLC = rawEventDataStreamForSLC
        .map(new EventMapFunction())
        .setParallelism(AppEnv.config().getFlink().getApp().getEventParallelism())
        .name("Event Operator For SLC")
        .slotSharingGroup("SLC")
        .uid("eventLevelForSLC");

    // union ubiEvent from SLC/RNO/LVS
    DataStream<UbiEvent> ubiEventDataStream = ubiEventDataStreamForLVS
        .union(ubiEventDataStreamForSLC)
        .union(ubiEventDataStreamForRNO);

    // refine windowsoperatorø
    // 3. Session Operator
    // 3.1 Session window
    // 3.2 Session indicator accumulation
    // 3.3 Session Level bot detection (via bot rule & signature)
    // 3.4 Event level bot detection (via session flag)
    OutputTag<UbiSession> sessionOutputTag =
        new OutputTag<>("session-output-tag", TypeInformation.of(UbiSession.class));
    OutputTag<UbiEvent> lateEventOutputTag =
        new OutputTag<>("late-event-output-tag", TypeInformation.of(UbiEvent.class));

    OutputTag<UbiEvent> mappedEventOutputTag =
        new OutputTag<>("mapped-event-output-tag", TypeInformation.of(UbiEvent.class));
    SingleOutputStreamOperator<UbiSession> ubiSessionDataStream =
        ubiEventDataStream
            .keyBy("guid")
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .allowedLateness(Time.minutes(1))
            .sideOutputLateData(lateEventOutputTag)
            .aggregate(new UbiSessionAgg(), new UbiSessionWindowProcessFunction());

    WindowOperatorHelper.enrichWindowOperator(
        (OneInputTransformation) ubiSessionDataStream.getTransformation(),
        new UbiEventMapWithStateFunction(),
        mappedEventOutputTag);

    ubiSessionDataStream
        .setParallelism(AppEnv.config().getFlink().app.getSessionParallelism())
        .name("Session Operator")
        .slotSharingGroup("SESSION")
        .uid("sessionLevel");

    DataStream<UbiEvent> ubiEventWithSessionId =
        ubiSessionDataStream.getSideOutput(mappedEventOutputTag);

    DataStream<UbiEvent> latedStream =
        ubiSessionDataStream.getSideOutput(lateEventOutputTag);

    // ubiSession to intermediateSession
    DataStream<IntermediateSession> intermediateSessionDataStream = ubiSessionDataStream
        .map(new UbiSessionToIntermediateSessionMapFunction())
        .setParallelism(AppEnv.config().getFlink().app.getSessionParallelism())
        .name("UbiSession To IntermediateSession")
        .slotSharingGroup("SESSION")
        .uid("CrossSessionLevel");

    /*
    DataStream<UbiSessionForDQ> crossSessionForDQSreaming = ubiSessionDataStream
        .map(new UbiSessionToUbiSessionForDQMapFunction())
        .setParallelism(AppEnv.config().getFlink().app.getSessionParallelism())
        .name("UbiSession for cross session dq")
        .slotSharingGroup("SESSION")
        .uid("CrossLevel");

    // ubiSession to SessionForGuidEnhancement
    SingleOutputStreamOperator<SessionForGuidEnhancement> sessionForGuidEnhancement =
        ubiSessionDataStream
            .map(new UbiSessionForGuidEnhancementMapFunction())
            .setParallelism(AppEnv.config().getFlink().app.getSessionParallelism())
            .name("ubiSession to SessionForGuidEnhancement")
            .uid("enhanceGuid");
            */

    // 4. Attribute Operator
    // 4.1 Sliding window
    // 4.2 Attribute indicator accumulation
    // 4.3 Attribute level bot detection (via bot rule)
    // 4.4 Store bot signature
    /*
    DataStream<AgentIpAttribute> agentIpAttributeDatastream =
        intermediateSessionDataStream
            .keyBy("userAgent", "clientIp")
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .aggregate(new AgentIpAttributeAgg(), new AgentIpWindowProcessFunction())
            .name("Attribute Operator (Agent+IP Pre-Aggregation)")
            .setParallelism(AppEnv.config().getFlink().app.getPreAgentIpParallelism())
            .uid("preAgentIpLevel");

    DataStream<Tuple4<String, Boolean, Set<Integer>, Long>> guidSignatureDataStream =
        intermediateSessionDataStream
            .keyBy("guid1", "guid2")
            .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
            .trigger(OnElementEarlyFiringTrigger.create())
            .aggregate(new GuidAttributeAgg(), new GuidWindowProcessFunction())
            .name("Attribute Operator (GUID)")
            .setParallelism(AppEnv.config().getFlink().app.getGuidParallelism())
            .uid("guidLevel");

    SplitStream<Tuple4<String, Boolean, Set<Integer>, Long>> guidSignatureSplitStream =
        guidSignatureDataStream.split(new SplitFunction());

    guidSignatureSplitStream
        .select("generation")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getGuidParallelism())
        .name("GUID Signature Generation")
        .uid("generationGuidLevel");

    guidSignatureSplitStream
        .select("expiration")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getGuidParallelism())
        .name("GUID Signature Expiration")
        .uid("expirationGuidLevel");

    DataStream<Tuple4<String, Boolean, Set<Integer>, Long>> agentIpSignatureDataStream =
        agentIpAttributeDatastream
            .keyBy("agent", "clientIp")
            .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
            .trigger(OnElementEarlyFiringTrigger.create())
            .aggregate(
                new AgentIpAttributeAggSliding(), new AgentIpSignatureWindowProcessFunction())
            .name("Attribute Operator (Agent+IP)")
            .setParallelism(AppEnv.config().getFlink().app.getAgentIpParallelism())
            .uid("agentIpLevel");

    SplitStream<Tuple4<String, Boolean, Set<Integer>, Long>> agentIpSignatureSplitStream =
        agentIpSignatureDataStream.split(new SplitFunction());

    agentIpSignatureSplitStream
        .select("generation")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getAgentIpParallelism())
        .name("Agent+IP Signature Generation")
        .uid("generationAgentIpLevel");

    agentIpSignatureSplitStream
        .select("expiration")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getAgentIpParallelism())
        .name("Agent+IP Signature Expiration")
        .uid("expirationAgentIpLevel");

    DataStream<Tuple4<String, Boolean, Set<Integer>, Long>> agentSignatureDataStream =
        agentIpAttributeDatastream
            .keyBy("agent")
            .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
            .trigger(OnElementEarlyFiringTrigger.create())
            .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
            .name("Attribute Operator (Agent)")
            .setParallelism(AppEnv.config().getFlink().app.getAgentParallelism())
            .uid("agentLevel");

    SplitStream<Tuple4<String, Boolean, Set<Integer>, Long>> agentSignatureSplitStream =
        agentSignatureDataStream.split(new SplitFunction());

    agentSignatureSplitStream
        .select("generation")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getAgentParallelism())
        .name("Agent Signature Generation")
        .uid("generationAgentLevel");

    agentSignatureSplitStream
        .select("expiration")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getAgentParallelism())
        .name("Agent Signature Expiration")
        .uid("expirationAgentLevel");

    DataStream<Tuple4<String, Boolean, Set<Integer>, Long>> ipSignatureDataStream =
        agentIpAttributeDatastream
            .keyBy("clientIp")
            .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(12), Time.hours(7)))
            .trigger(OnElementEarlyFiringTrigger.create())
            .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
            .name("Attribute Operator (IP)")
            .setParallelism(AppEnv.config().getFlink().app.getIpParallelism())
            .uid("ipLevel");

    SplitStream<Tuple4<String, Boolean, Set<Integer>, Long>> ipSignatureSplitStream =
        ipSignatureDataStream.split(new SplitFunction());

    ipSignatureSplitStream
        .select("generation")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getIpParallelism())
        .name("IP Signature Generation")
        .uid("generationIpLevel");

    ipSignatureSplitStream
        .select("expiration")
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getIpParallelism())
        .name("IP Signature Expiration")
        .uid("expirationIpLevel");

    // union attribute signature for broadcast
    DataStream<Tuple4<String, Boolean, Set<Integer>, Long>> attributeSignatureDataStream =
        agentIpSignatureDataStream
            .union(agentSignatureDataStream)
            .union(ipSignatureDataStream)
            .union(guidSignatureDataStream);

    // attribute signature broadcast
    BroadcastStream<Tuple4<String, Boolean, Set<Integer>, Long>> attributeSignatureBroadcastStream =
        attributeSignatureDataStream.broadcast(MapStateDesc.attributeSignatureDesc);

    // transform ubiEvent,ubiSession to same type and union
    DataStream<Either<UbiEvent, UbiSession>> detectableDataStream =
        ubiSessionDataStream
            .map(new DetectableSessionMapFunction())
            .setParallelism(AppEnv.config().getFlink().app.getSessionParallelism())
            .union(ubiEventWithSessionId
                .map(new DetectableEventMapFunction())
                .setParallelism(AppEnv.config().getFlink().app.getSessionParallelism()));

    // connect ubiEvent,ubiSession DataStream and broadcast Stream
    SingleOutputStreamOperator<UbiEvent> signatureBotDetectionForEvent =
        detectableDataStream
            .connect(attributeSignatureBroadcastStream)
            .process(new AttributeBroadcastProcessFunctionForDetectable(sessionOutputTag))
            .setParallelism(AppEnv.config().getFlink().app.getBroadcastParallelism())
            .name("Signature Bot Detector")
            .uid("connectLevel");

    DataStream<UbiSession> signatureBotDetectionForSession = signatureBotDetectionForEvent
        .getSideOutput(sessionOutputTag);
        */

    /*
    // UbiSession to SojSession
    DataStream<SojSession> sojSessionStream =
        signatureBotDetectionForSession
            .map(new UbiSessionToSojSessionMapFunction())
            .setParallelism(AppEnv.config().getFlink().app.getBroadcastParallelism())
            .name("UbiSession to SojSession")
            .uid("ubiSessionToSojSession");

    // UbiEvent to SojEvent
    DataStream<SojEvent> sojEventWithSessionId =
        signatureBotDetectionForEvent
            .map(new UbiEventToSojEventMapFunction())
            .setParallelism(AppEnv.config().getFlink().app.getBroadcastParallelism())
            .name("UbiEvent to SojEvent")
            .uid("ubiEventToSojEvent");
            */

    // 5. Load data to file system for batch processing
    // 5.1 IP Signature
    // 5.2 Sessions (ended)
    // 5.3 Events (with session ID & bot flags)
    // 5.4 Events late
    // for data quality

    // kafka sink for session
    /*
    sojSessionStream.addSink(KafkaConnectorFactory
        .createKafkaProducer(Constants.TOPIC_PRODUCER_SESSION, Constants.BOOTSTRAP_SERVERS_SESSION,
            SojSession.class, Constants.MESSAGE_KEY))
        .setParallelism(AppEnv.config().getFlink().app.getSessionKafkaParallelism())
        .name("SojSession Kafka")
        .uid("kafkaSink");
        */

    // metrics collector for end to end
    ubiEventWithSessionId
        .process(new PipelineMetricsCollectorProcessFunction())
        .setParallelism(AppEnv.config().getFlink().app.getMetricsParallelism())
        .name("Pipeline End to End Duration")
        .slotSharingGroup("SESSION")
        .uid("endToEndMetricsCollector");

    // metrics collector for event rules hit
    ubiEventWithSessionId
        .process(new EventMetricsCollectorProcessFunction())
        .setParallelism(AppEnv.config().getFlink().app.getMetricsParallelism())
        .name("Event Metrics Collector")
        .slotSharingGroup("SESSION")
        .uid("eventLevelMetricsCollector");

    // late event sink
    latedStream
        .addSink(new DiscardingSink<>())
        .setParallelism(AppEnv.config().getFlink().app.getSessionParallelism())
        .name("Late Event")
        .slotSharingGroup("SESSION")
        .uid("lateEvent");

    // sink kafka for cross session DQ
    intermediateSessionDataStream
        .addSink(KafkaConnectorFactory
            .createKafkaProducer(Constants.TOPIC_PRODUCER_CROSS_SESSION_DQ,
                Constants.BOOTSTRAP_SERVERS_CROSS_SESSION_DQ,
                IntermediateSession.class, Constants.MESSAGE_KEY))
        .setParallelism(AppEnv.config().getFlink().app.getCrossSessionParallelism())
        .name("IntermediateSession Kafka")
        .slotSharingGroup("SESSION")
        .uid("kafkaSinkForSession");
    // Submit this job
    executionEnvironment.execute(AppEnv.config().getFlink().getApp().getNameForFullPipeline());
  }
}
