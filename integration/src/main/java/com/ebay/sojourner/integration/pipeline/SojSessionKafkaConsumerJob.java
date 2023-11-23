package com.ebay.sojourner.integration.pipeline;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_NAME;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_DC;
import static com.ebay.sojourner.common.util.Property.SOURCE_PARALLELISM;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.integration.schema.SojSessionRawSojSessionDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class SojSessionKafkaConsumerJob {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    SourceDataStreamBuilder<SojSession> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    String rheosRegistry = getString(Property.RHEOS_KAFKA_REGISTRY_URL);

    DataStream<SojSession> sourceDataStream = dataStreamBuilder
        .dc(DataCenter.of(getString(FLINK_APP_SOURCE_DC)))
        .parallelism(getInteger(SOURCE_PARALLELISM))
        .operatorName("Kafka Source")
        .uid("source")
        .build(new SojSessionRawSojSessionDeserializationSchema(rheosRegistry));

    sourceDataStream.addSink(new DiscardingSink<>())
                    .setParallelism(getInteger(SOURCE_PARALLELISM))
                    .name("Sink")
                    .uid("sink");

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, getString(FLINK_APP_NAME));
  }
}
