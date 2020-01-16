package com.ebay.sojourner.ubd.rt.pipeline;

import com.ebay.sojourner.ubd.common.model.*;
import com.ebay.sojourner.ubd.rt.common.broadcast.AgentBroadcastProcessFunction;
import com.ebay.sojourner.ubd.rt.common.broadcast.AgentIpBroadcastProcessFunction;
import com.ebay.sojourner.ubd.rt.common.broadcast.IpBroadcastProcessFunction;
import com.ebay.sojourner.ubd.rt.common.state.MapStateDesc;
import com.ebay.sojourner.ubd.rt.common.state.StateBackendFactory;
import com.ebay.sojourner.ubd.rt.common.windows.OnElementEarlyFiringTrigger;
import com.ebay.sojourner.ubd.rt.connectors.kafka.KafkaConnectorFactory;
import com.ebay.sojourner.ubd.rt.operators.attribute.*;
import com.ebay.sojourner.ubd.rt.operators.event.EventMapFunction;
import com.ebay.sojourner.ubd.rt.operators.event.UbiEventMapWithStateFunction;
import com.ebay.sojourner.ubd.rt.operators.session.UbiSessionAgg;
import com.ebay.sojourner.ubd.rt.operators.session.UbiSessionWindowProcessFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorHelper;
import org.apache.flink.util.OutputTag;


public class SojournerUBDRTJob {

    public static void main( String[] args ) throws Exception {
        // 0.0 Prepare execution environment
        // 0.1 UBI configuration
        // 0.2 Flink configuration
//        InputStream resourceAsStream = SojournerUBDRTJob.class.getResourceAsStream("/ubi.properties");
//        UBIConfig ubiConfig = UBIConfig.getInstance(resourceAsStream);
//        Method m = TypeExtractor.class.getDeclaredMethod("registerFactory", Type.class, Class.class);
//        m.setAccessible(true);
//        m.invoke(null, String.class, SOjStringFactory.class);
        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
//        final ParameterTool params = ParameterTool.fromArgs(args);
//        executionEnvironment.getConfig().setGlobalJobParameters(new SojJobParameters());
        // LookupUtils.uploadFiles(executionEnvironment, params, ubiConfig);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.getConfig().setLatencyTrackingInterval(2000);
        executionEnvironment.enableCheckpointing(180 * 1000);
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);
        executionEnvironment.setStateBackend(
                StateBackendFactory.getStateBackend(StateBackendFactory.ROCKSDB));
        executionEnvironment.setParallelism(2);

        // 1. Rheos Consumer
        // 1.1 Consume RawEvent from Rheos PathFinder topic
        // 1.2 Assign timestamps and emit watermarks.
        DataStream<RawEvent> rawEventDataStream = executionEnvironment.addSource(
                KafkaConnectorFactory.createKafkaConsumer().assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<RawEvent>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp( RawEvent element ) {
                                return element.getRheosHeader().getEventCreateTimestamp();
                            }
                        }
                ))
                .name("Rheos Consumer");

        // 2. Event Operator
        // 2.1 Parse and transform RawEvent to UbiEvent
        // 2.2 Event level bot detection via bot rule
        DataStream<UbiEvent> ubiEventDataStream = rawEventDataStream
                .map(new EventMapFunction())
                .name("Event Operator")
                .startNewChain();


        //refine window operator

        // 3. Session Operator
        // 3.1 Session window
        // 3.2 Session indicator accumulation
        // 3.3 Session Level bot detection (via bot rule & signature)
        // 3.4 Event level bot detection (via session flag)
        OutputTag<UbiSession> sessionOutputTag =
                new OutputTag<>("session-output-tag", TypeInformation.of(UbiSession.class));
        OutputTag<UbiEvent> lateEventOutputTag =
                new OutputTag<>("late-event-output-tag", TypeInformation.of(UbiEvent.class));
//        JobID jobId = executionEnvironment.getStreamGraph().getJobGraph().getJobID();

        OutputTag<UbiEvent> mappedEventOutputTag =
                new OutputTag<>("mapped-event-output-tag", TypeInformation.of(UbiEvent.class));
        SingleOutputStreamOperator<UbiSession> ubiSessinDataStream = ubiEventDataStream
                .keyBy("guid")
                .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
//                .trigger(OnElementEarlyFiringTrigger.create())   //no need to customize the triiger, use the default eventtimeTrigger
                .allowedLateness(Time.hours(1))
                .sideOutputLateData(lateEventOutputTag)
                .aggregate(new UbiSessionAgg(),
                        new UbiSessionWindowProcessFunction());

        WindowOperatorHelper.enrichWindowOperator(
                (OneInputTransformation) ubiSessinDataStream.getTransformation(),
                new UbiEventMapWithStateFunction(),
                mappedEventOutputTag
        );

        ubiSessinDataStream.name("Session Operator");

        DataStream<UbiEvent> mappedEventStream = ubiSessinDataStream.getSideOutput(mappedEventOutputTag);

        // 4. Attribute Operator
        // 4.1 Sliding window
        // 4.2 Attribute indicator accumulation
        // 4.3 Attribute level bot detection (via bot rule)
        // 4.4 Store bot signature
        DataStream<AgentIpAttribute> agentIpAttributeDataStream = ubiSessinDataStream
                .keyBy("userAgent", "clientIp")
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
                .trigger(OnElementEarlyFiringTrigger.create())
                .aggregate(new AgentIpAttributeAgg(), new AgentIpWindowProcessFunction())
                .name("Attribute Operator (Agent+IP)");

        // agent ip DataStream & agent ip bot dectector
        SingleOutputStreamOperator<AgentIpSignature> agentIpSignatureDataStream = agentIpAttributeDataStream
                .keyBy("agent", "clientIp")
                .map(new AgentIpMapFunction())
                .name("Signature Generate(Agent+IP)");

        agentIpSignatureDataStream.print().name("Agent+IP Signature");

        DataStream<AgentSignature> agentAttributeDataStream = agentIpAttributeDataStream
                .keyBy("agent")
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
                .trigger(OnElementEarlyFiringTrigger.create())
                .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
                .name("Attribute Operator (Agent)");

        agentAttributeDataStream.print().name("Agent Signature");

        DataStream<IpSignature> ipAttributeDataStream = agentIpAttributeDataStream
                .keyBy("clientIp")
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
                .trigger(OnElementEarlyFiringTrigger.create())
                .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
                .name("Attribute Operator (IP)");

        // agent ip broadcast
        BroadcastStream<AgentIpSignature> agentIpBroadcastStream = agentIpSignatureDataStream.broadcast(MapStateDesc.agentIpSignatureDesc);

        // agent broadcast
        BroadcastStream<AgentSignature> agentBroadcastStream = agentAttributeDataStream.broadcast(MapStateDesc.agentSignatureDesc);

        // ip broadcast
        BroadcastStream<IpSignature> ipBroadcastStrem = ipAttributeDataStream.broadcast(MapStateDesc.ipSignatureDesc);

        SingleOutputStreamOperator<UbiEvent> ipConnectDataStream = mappedEventStream
                .connect(ipBroadcastStrem)
                .process(new IpBroadcastProcessFunction())
                .name("Signature BotDetection(IP)");

        SingleOutputStreamOperator<UbiEvent> agentConnectDataStream = ipConnectDataStream
                .connect(agentBroadcastStream)
                .process(new AgentBroadcastProcessFunction())
                .name("Signature BotDetection(Agent)");

        SingleOutputStreamOperator<UbiEvent> agentIpConnectDataStream = agentConnectDataStream
                .connect(agentIpBroadcastStream)
                .process(new AgentIpBroadcastProcessFunction())
                .name("Signature BotDetection(Agent+IP)");

        // 5. Load data to file system for batch processing
        // 5.1 IP Signature
        // 5.2 Sessions (ended)
        // 5.3 Events (with session ID & bot flags)
        // 5.4 Events late
//        ipAttributeDataStream.addSink(StreamingFileSinkFactory.ipSignatureSinkWithAP())
//                .name("IP Signature").disableChaining();
//        sessionStream.addSink(StreamingFileSinkFactory.sessionSink())
//                .name("Sessions").disableChaining();
//        ubiEventStreamWithSessionId.addSink(StreamingFileSinkFactory.eventSink())
//                .name("Events").disableChaining();

        DataStream<UbiEvent> lateEventStream =
                ubiSessinDataStream.getSideOutput(lateEventOutputTag);

//        lateEventStream.addSink(StreamingFileSinkFactory.lateEventSink())
//                .name("Events (Late)").disableChaining();

        ipAttributeDataStream.print().name("IP Signature");
//        agentIpAttributeDataStream.print().name("AgentIp Signature").disableChaining();

        ubiSessinDataStream.print().name("Sessions").disableChaining();
        agentIpConnectDataStream.print().name("Events").disableChaining();
//        ubiEventStreamWithSessionId.print().name("Events").disableChaining();
        lateEventStream.print().name("Events (Late)").disableChaining();

        // Submit this job
        executionEnvironment.execute("Unified Bot Detection RT Pipeline");

    }

}
