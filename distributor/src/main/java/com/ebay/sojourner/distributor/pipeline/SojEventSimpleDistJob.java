package com.ebay.sojourner.distributor.pipeline;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_NAME;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SINK_KAFKA_TOPIC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_DC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_OP_NAME;
import static com.ebay.sojourner.common.util.Property.SINK_KAFKA_PARALLELISM;
import static com.ebay.sojourner.common.util.Property.SOURCE_PARALLELISM;
import static com.ebay.sojourner.common.util.Property.MAX_MESSAGE_BYTES;
import static com.ebay.sojourner.common.util.Property.DEBUG_MODE;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getBoolean;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getList;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getLong;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getStringList;

import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.distributor.function.AddTagMapFunction;
import com.ebay.sojourner.distributor.function.CFlagFilterFunction;
import com.ebay.sojourner.distributor.function.SimpleDistSojEventWrapperProcessFunction;
import com.ebay.sojourner.distributor.function.SinkDataStreamBuilder;
import com.ebay.sojourner.distributor.function.SojEventDistByColoProcessFunction;
import com.ebay.sojourner.distributor.schema.bullseye.BullseyeSojEventDeserializationSchema;
import com.ebay.sojourner.distributor.schema.bullseye.SimpleDistSojEventWrapperSerializationSchema;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojEventSimpleDistJob {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    final String DATA_SOURCE_OP_NAME = getString(FLINK_APP_SOURCE_OP_NAME);
    final String DATA_SOURCE_UID = "sojevent-dist-source";
    final String FILTER_CFLAG_OP_NAME = "Filter Out Cflag SojEvent";
    final String FILTER_CFLAG_UID = "filter-out-cflag-sojevent";
    final String ADD_TAG_OP_NAME = "Add Tag";
    final String ADD_TAG_OP_UID = "add-tag";
    final String MAP_RAWSOJEVENTWRAPPER_OP_NAME = "SojEvent To SimpleDistSojEventWrapper";
    final String MAP_RAWSOJEVENTWRAPPER_OP_UID = "sojevent-to-simpledistsojeventwrapper";
    final String DIST_OP_NAME = "SimpleDistSojEventWrapper Distribution";
    final String DIST_UID = "SimpleDistSojEventWrapper-distribution";
    final String SINK_RNO_OP_NAME = "SojEvent Dist Sink RNO";
    final String SINK_RNO_UID = "sojevent-dist-sink-rno";
    final String SINK_SLC_OP_NAME = "SojEvent Dist Sink SLC";
    final String SINK_SLC_UID = "sojevent-dist-sink-slc";
    final String SINK_LVS_OP_NAME = "SojEvent Dist Sink LVS";
    final String SINK_LVS_UID = "sojevent-dist-sink-lvs";
    final String FLINK_APP_FILTER_TOPIC_CONFIG_KEY = "flink.app.filter.topic-config";

    SourceDataStreamBuilder<SojEvent> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<SojEvent> sojEventSourceDataStream = dataStreamBuilder
        .dc(DataCenter.of(getString(FLINK_APP_SOURCE_DC)))
        .parallelism(getInteger(SOURCE_PARALLELISM))
        .operatorName(DATA_SOURCE_OP_NAME)
        .uid(DATA_SOURCE_UID)
        .build(new BullseyeSojEventDeserializationSchema());

    DataStream<SojEvent> sojEventFilteredDataStream = sojEventSourceDataStream
        .filter(new CFlagFilterFunction())
        .name(FILTER_CFLAG_OP_NAME)
        .uid(FILTER_CFLAG_UID)
        .setParallelism(getInteger(SOURCE_PARALLELISM));

    DataStream<SojEvent> sojEventDataStream = sojEventFilteredDataStream
        .map(new AddTagMapFunction())
        .name(ADD_TAG_OP_NAME)
        .uid(ADD_TAG_OP_UID)
        .setParallelism(getInteger(SOURCE_PARALLELISM));

    DataStream<SimpleDistSojEventWrapper> enrichedSimpleDistSojEventWrapperDataStream =
        sojEventDataStream
            .process(new SimpleDistSojEventWrapperProcessFunction(
                getStringList(Property.FLINK_APP_SINK_KAFKA_MESSAGE_KEY_EVENT, ","),
                getList(FLINK_APP_FILTER_TOPIC_CONFIG_KEY),
                getLong(MAX_MESSAGE_BYTES),
                getBoolean(DEBUG_MODE)))
            .name(MAP_RAWSOJEVENTWRAPPER_OP_NAME)
            .uid(MAP_RAWSOJEVENTWRAPPER_OP_UID)
            .setParallelism(getInteger(SOURCE_PARALLELISM));

    SingleOutputStreamOperator<SimpleDistSojEventWrapper> sojEventDistStream =
        enrichedSimpleDistSojEventWrapperDataStream
            .process(
                new SojEventDistByColoProcessFunction(
                    getList(Property.DIST_HASH_KEY),
                    getList(Property.DIST_DC)))
            .name(DIST_OP_NAME)
            .uid(DIST_UID)
            .setParallelism(getInteger(SOURCE_PARALLELISM));

    // get side data stream for different dc
    DataStream<SimpleDistSojEventWrapper> rnoSojEventDistStream = sojEventDistStream
        .getSideOutput(OutputTagConstants.rnoDistOutputTag);
    DataStream<SimpleDistSojEventWrapper> lvsSojEventDistStream = sojEventDistStream
        .getSideOutput(OutputTagConstants.lvsDistOutputTag);
    DataStream<SimpleDistSojEventWrapper> slcSojEventDistStream = sojEventDistStream
        .getSideOutput(OutputTagConstants.slcDistOutputTag);

    // sink to kafka
    SinkDataStreamBuilder<SimpleDistSojEventWrapper> sinkDataStreamBuilder
        = new SinkDataStreamBuilder<>();
    List<String> dcList = getList(Property.DIST_DC);

    if (dcList.contains("rno")) {
      sinkDataStreamBuilder
          .stream(rnoSojEventDistStream)
          .topic(getString(FLINK_APP_SINK_KAFKA_TOPIC))
          .dc(DataCenter.RNO)
          .parallelism(getInteger(SINK_KAFKA_PARALLELISM))
          .operatorName(SINK_RNO_OP_NAME)
          .uid(SINK_RNO_UID)
          .build(new SimpleDistSojEventWrapperSerializationSchema());
    }

    if (dcList.contains("lvs")) {
      sinkDataStreamBuilder
          .stream(lvsSojEventDistStream)
          .topic(getString(FLINK_APP_SINK_KAFKA_TOPIC))
          .dc(DataCenter.LVS)
          .parallelism(getInteger(SINK_KAFKA_PARALLELISM))
          .operatorName(SINK_LVS_OP_NAME)
          .uid(SINK_LVS_UID)
          .build(new SimpleDistSojEventWrapperSerializationSchema());
    }

    if (dcList.contains("slc")) {
      sinkDataStreamBuilder
          .stream(slcSojEventDistStream)
          .topic(getString(FLINK_APP_SINK_KAFKA_TOPIC))
          .dc(DataCenter.SLC)
          .parallelism(getInteger(SINK_KAFKA_PARALLELISM))
          .operatorName(SINK_SLC_OP_NAME)
          .uid(SINK_SLC_UID)
          .build(new SimpleDistSojEventWrapperSerializationSchema());
    }

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, getString(FLINK_APP_NAME));
  }

}
