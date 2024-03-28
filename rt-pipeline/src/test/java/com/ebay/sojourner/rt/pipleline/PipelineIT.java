package com.ebay.sojourner.rt.pipleline;

import static org.junit.Assert.assertTrue;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.AgentIpSignature;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.rt.operator.event.UbiEventMapWithStateFunction;
import com.ebay.sojourner.rt.operator.session.UbiSessionAgg;
import com.ebay.sojourner.rt.operator.session.UbiSessionWindowProcessFunction;
import com.ebay.sojourner.rt.util.RawEventGenerator;
import com.ebay.sojourner.rt.operator.AgentIpMapFunction;
import com.ebay.sojourner.rt.operator.attribute.AgentIpAttributeAgg;
import com.ebay.sojourner.rt.operator.attribute.AgentIpWindowProcessFunction;
import com.ebay.sojourner.rt.operator.event.EventMapFunction;
import com.ebay.sojourner.rt.operator.session.UbiSessionToSessionCoreMapFunction;
import com.ebay.sojourner.rt.util.ResultGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorHelper;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.OutputTag;
import org.junit.ClassRule;
import org.junit.Test;

public class PipelineIT {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  @Test
  public void testIncrementPipeline() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // configure your test environment
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // values are collected in a static variable
    CollectSink.values.clear();

    // create a stream of custom elements and apply transformations
    //        DataStream<RawEvent> rawEventDataStream =
    // env.fromCollection(RawEventGenerator.getRawEventList("/SourceData.json")).
    // assignTimestampsAndWatermarks
    //                (
    //                        new
    // BoundedOutOfOrdernessTimestampExtractor<RawEvent>(Time.seconds(10)) {
    //                            @Override
    //                            public long extractTimestamp( RawEvent element ) {
    //                                return element.getRheosHeader().getEventCreateTimestamp();
    //                            }
    //                        }
    //                ).name("Source Operator");

    // create a stream of custom elements and apply transformations
    DataStream<RawEvent> rawEventDataStream =
        env.addSource(new DynamicConfigSource())
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<RawEvent>(Time.seconds(10)) {
                  @Override
                  public long extractTimestamp(RawEvent element) {
                    return element.getRheosHeader().getEventCreateTimestamp();
                  }
                })
            .name("Source Operator");
    DataStream<UbiEvent> ubiEventDataStream =
        rawEventDataStream
            .map(new EventMapFunction(Maps.newHashMap()))
            .setParallelism(2)
            .name("Event Operator"); // .slotSharingGroup("event");
    //        ubiEventDataStream.addSink(new CollectSink2());
    //        ubiEventDataStream.print();
    OutputTag<UbiSession> sessionOutputTag =
        new OutputTag<>("session-output-tag", TypeInformation.of(UbiSession.class));
    OutputTag<UbiEvent> lateEventOutputTag =
        new OutputTag<>("late-event-output-tag", TypeInformation.of(UbiEvent.class));
    //        JobID jobId = executionEnvironment.getStreamGraph().getJobGraph().getJobID();∂
    OutputTag<UbiEvent> mappedEventOutputTag =
        new OutputTag<>("mapped-event-output-tag", TypeInformation.of(UbiEvent.class));
    SingleOutputStreamOperator<UbiSession> ubiSessinDataStream =
        ubiEventDataStream
            .keyBy("guid")
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            //                .trigger(OnElementEarlyFiringTrigger.create())   //no need to
            // customize the triiger, use the default eventtimeTrigger
            .allowedLateness(Time.hours(1))
            .sideOutputLateData(lateEventOutputTag)
            .aggregate(new UbiSessionAgg(), new UbiSessionWindowProcessFunction());

    WindowOperatorHelper.enrichWindowOperator(
        (OneInputTransformation) ubiSessinDataStream.getTransformation(),
        new UbiEventMapWithStateFunction(),
        mappedEventOutputTag);

    ubiSessinDataStream.name("Session Operator");

    DataStream<UbiEvent> mappedEventStream =
        ubiSessinDataStream.getSideOutput(mappedEventOutputTag);

    // ubiSession to intermediateSession
    DataStream<SessionCore> intermediateSessionDataStream = ubiSessinDataStream
        .map(new UbiSessionToSessionCoreMapFunction())
        .setParallelism(2)
        .name("UbiSession To IntermediateSession")
        .slotSharingGroup("SESSION")
        .uid("CrossSessionLevel");

    //        ubiEventStreamWithSessionId.print();
    // 4. Attribute Operator
    // 4.1 Sliding window
    // 4.2 Attribute indicator accumulation
    // 4.3 Attribute level bot detection (via bot rule)
    // 4.4 Store bot signature
    DataStream<AgentIpAttribute> agentIpAttributeDataStream =
        intermediateSessionDataStream
            .keyBy("userAgent", "ip")
            .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
            //                .trigger(OnElementEarlyFiringTrigger.create())
            .aggregate(new AgentIpAttributeAgg(), new AgentIpWindowProcessFunction())
            .name("Attribute Operator (Agent+IP)");
    //        agentIpAttributeDataStream.print();

    // agent ip DataStream & agent ip bot dectector
    SingleOutputStreamOperator<AgentIpSignature> agentIpSignatureDataStream =
        agentIpAttributeDataStream
            .keyBy("agent", "clientIp")
            .map(new AgentIpMapFunction())
            .name("Signature Generate(Agent+IP)");

    //        agentIpSignatureDataStream.print().name("Agent+IP Signature");

    //        DataStream<AgentSignature> agentAttributeDataStream = agentIpAttributeDataStream
    //                .keyBy("agent")
    //                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
    ////                .trigger(OnElementEarlyFiringTrigger.create())
    //                .aggregate(new AgentAttributeAgg(), new AgentWindowProcessFunction())
    //                .name("Attribute Operator (Agent)");
    //
    ////        agentAttributeDataStream.print().name("Agent Signature");
    //
    //        DataStream<IpSignature> ipAttributeDataStream = agentIpAttributeDataStream
    //                .keyBy("clientIp")
    //                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
    ////                .trigger(OnElementEarlyFiringTrigger.create())
    //                .aggregate(new IpAttributeAgg(), new IpWindowProcessFunction())
    //                .name("Attribute Operator (IP)");

    //        ipAttributeDataStream.print().name("ipSigunature");
    // agent ip broadcast
    //        BroadcastStream<AgentIpSignature> agentIpBroadcastStream =
    // agentIpSignatureDataStream.broadcast(MapStateDesc.agentIpSignatureDesc);
    //
    //        // agent broadcast
    //        BroadcastStream<AgentSignature> agentBroadcastStream =
    // agentAttributeDataStream.broadcast(MapStateDesc.agentSignatureDesc);
    //
    //        // ip broadcast
    //        BroadcastStream<IpSignature> ipBroadcastStrem =
    // ipAttributeDataStream.broadcast(MapStateDesc.ipSignatureDesc);

    //        SingleOutputStreamOperator<UbiEvent> ipConnectDataStream = mappedEventStream
    //                .connect(ipBroadcastStrem)
    //                .process(new IpBroadcastProcessFunction())
    //                .name("Signature BotDetection(IP)");
    //
    //        SingleOutputStreamOperator<UbiEvent> agentConnectDataStream = ipConnectDataStream
    //                .connect(agentBroadcastStream)
    //                .process(new AgentBroadcastProcessFunction())
    //                .name("Signature BotDetection(Agent)");
    //
    //        SingleOutputStreamOperator<UbiEvent> agentIpConnectDataStream = agentConnectDataStream
    //                .connect(agentIpBroadcastStream)
    //                .process(new AgentIpBroadcastProcessFunction())
    //                .name("Signature BotDetection(Agent+IP)").slotSharingGroup("session");
    //        SingleOutputStreamOperator<UbiSession> ipConnectDataStreamForSession = sessionStream
    //                .connect(ipBroadcastStrem)
    //                .process(new IpBroadcastProcessFunctionForSession())
    //                .name("Signature BotDetection(IP) for UbiSession");

    ubiSessinDataStream.addSink(new CollectSink());

    //        ubiEventDataStream.print().name("event");
    //        sessionStream.print().name("test");
    //        ipConnectDataStream.print().name("test");
    //        ipConnectDataStreamForSession.print();
    // execute
    JobExecutionResult jobExecutionResult = env.execute();
    jobExecutionResult.isJobExecutionResult();

    List<UbiSession> ubiSessions = ResultGenerator.getUbiSessionList("/ExpectedData.json");

    // verify your results
    assertTrue(CollectSink.values.containsAll(ubiSessions));
  }

  // create a testing sink
  private static class CollectSink implements SinkFunction<UbiSession> {

    // must be static
    public static final List<UbiSession> values = new ArrayList<>();

    @Override
    public synchronized void invoke(UbiSession value) throws Exception {
      values.add(value);
    }
  }

  public class DynamicConfigSource implements SourceFunction<RawEvent> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceFunction.SourceContext<RawEvent> ctx) throws Exception {
      long idx = 1;

      while (isRunning) {
        List<RawEvent> rawEvents = RawEventGenerator.getRawEventList("/SourceData.json" + idx);
        for (RawEvent rawEvent : rawEvents) {
          ctx.collect(rawEvent);
        }
        idx++;
        if (idx > 2) {
          isRunning = false;
        }
        //                TimeUnit.SECONDS.sleep(1);
        TimeUnit.SECONDS.sleep(60);
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
