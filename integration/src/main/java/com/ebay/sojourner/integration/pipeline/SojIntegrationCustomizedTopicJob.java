package com.ebay.sojourner.integration.pipeline;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_NAME;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_DC;
import static com.ebay.sojourner.common.util.Property.SOURCE_PARALLELISM;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.RheosEventDeserializationSchema;
import com.ebay.sojourner.integration.function.RheosEventToSojEventMapFunction;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class SojIntegrationCustomizedTopicJob {
  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    SourceDataStreamBuilder<RheosEvent> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<RheosEvent> rheosEventDataStream = dataStreamBuilder
        .dc(DataCenter.of(getString(FLINK_APP_SOURCE_DC)))
        .parallelism(getInteger(SOURCE_PARALLELISM))
        .operatorName("Source")
        .uid("source")
        .build(new RheosEventDeserializationSchema());


    DataStream<SojEvent> dataStream =
        rheosEventDataStream.map(new RheosEventToSojEventMapFunction(
            getString(Property.RHEOS_KAFKA_REGISTRY_URL)))
                            .setParallelism(getInteger(SOURCE_PARALLELISM))
                            .name("RheosEvent map to SojEvent")
                            .uid("map");


    dataStream.addSink(new DiscardingSink<>())
              .setParallelism(getInteger(SOURCE_PARALLELISM))
              .name("Discard Sink")
              .uid("discard-sink");

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, getString(FLINK_APP_NAME));
  }
}
