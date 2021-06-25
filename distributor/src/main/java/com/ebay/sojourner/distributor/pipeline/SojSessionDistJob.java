package com.ebay.sojourner.distributor.pipeline;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_NAME;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SINK_DC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SINK_KAFKA_TOPIC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SINK_OP_NAME;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_DC;
import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_OP_NAME;
import static com.ebay.sojourner.common.util.Property.SINK_KAFKA_PARALLELISM;
import static com.ebay.sojourner.common.util.Property.SOURCE_PARALLELISM;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.RawSojSessionWrapper;
import com.ebay.sojourner.distributor.function.SessionEnhanceMapFunction;
import com.ebay.sojourner.distributor.schema.RawSojSessionDeserializationSchema;
import com.ebay.sojourner.distributor.schema.RawSojSessionWrapperSerializationSchema;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.kafka.FlinkKafkaProducerFactory;
import com.ebay.sojourner.flink.connector.kafka.KafkaProducerConfig;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojSessionDistJob {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    final String DATA_SOURCE_OP_NAME = getString(FLINK_APP_SOURCE_OP_NAME);
    final String DATA_SOURCE_UID = "sojsession-dist-source";
    final String SINK_OP_NAME = getString(FLINK_APP_SINK_OP_NAME);
    final String SINK_UID = "sojsession-dist-sink";
    final String SESSION_ENHANCE_NAME = "SojSession Enhance";
    final String SESSION_ENHANCE_UID = "sojsession-enhance";

    SourceDataStreamBuilder<RawSojSessionWrapper> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<RawSojSessionWrapper> rawSojSessionWrapperDataStream = dataStreamBuilder
        .dc(DataCenter.of(getString(FLINK_APP_SOURCE_DC)))
        .parallelism(getInteger(SOURCE_PARALLELISM))
        .operatorName(DATA_SOURCE_OP_NAME)
        .uid(DATA_SOURCE_UID)
        .build(new RawSojSessionDeserializationSchema());

    DataStream<RawSojSessionWrapper> sessionEnhanceDataStream = rawSojSessionWrapperDataStream
        .map(new SessionEnhanceMapFunction())
        .setParallelism(getInteger(SOURCE_PARALLELISM))
        .name(SESSION_ENHANCE_NAME)
        .uid(SESSION_ENHANCE_UID);

    // sink to kafka
    KafkaProducerConfig config = KafkaProducerConfig.ofDC(getString(FLINK_APP_SINK_DC));
    FlinkKafkaProducerFactory producerFactory = new FlinkKafkaProducerFactory(config);
    sessionEnhanceDataStream
        .addSink(producerFactory.get(FlinkEnvUtils.getString(FLINK_APP_SINK_KAFKA_TOPIC),
            new RawSojSessionWrapperSerializationSchema(
                FlinkEnvUtils.getString(FLINK_APP_SINK_KAFKA_TOPIC))))
        .setParallelism(getInteger(SINK_KAFKA_PARALLELISM))
        .name(SINK_OP_NAME)
        .uid(SINK_UID);

    // Submit this job
    FlinkEnvUtils.execute(executionEnvironment, getString(FLINK_APP_NAME));
  }
}
