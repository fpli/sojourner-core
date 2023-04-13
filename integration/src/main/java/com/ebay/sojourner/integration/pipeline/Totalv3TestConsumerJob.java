package com.ebay.sojourner.integration.pipeline;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.PassThroughDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

public class Totalv3TestConsumerJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

        SourceDataStreamBuilder<byte[]> sourceStreamBuilder =
                new SourceDataStreamBuilder<>(executionEnvironment);

        DataStream<byte[]> sourceStream = sourceStreamBuilder
                .dc(DataCenter.of(getString(Property.FLINK_APP_SOURCE_DC)))
                .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
                .uid(getString(Property.SOURCE_UID))
                .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
                .build(new PassThroughDeserializationSchema());
        sourceStream.addSink(new DiscardingSink<>())
                .name(getString(Property.SINK_OPERATOR_NAME))
                .uid(getString(Property.SINK_UID))
                .setParallelism(getInteger(Property.SOURCE_PARALLELISM));

        FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
    }
}
