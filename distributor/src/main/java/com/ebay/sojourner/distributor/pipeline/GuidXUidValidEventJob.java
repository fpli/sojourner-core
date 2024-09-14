package com.ebay.sojourner.distributor.pipeline;

import com.ebay.sojourner.common.model.RawSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.function.*;
import com.ebay.sojourner.distributor.schema.deserialize.RawSojEventWrapperDeserializationSchema;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.SojEventKafkaRecordSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

import static com.ebay.sojourner.common.constant.ConfigProperty.*;
import static com.ebay.sojourner.flink.common.DataCenter.*;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class GuidXUidValidEventJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        final String REGISTRY_URL = flinkEnv.getString(RHEOS_REGISTRY_URL);
        final String TOPIC_EVENT = flinkEnv.getString("flink.app.sink.kafka.topic");
        final String SUBJECT_SOJEVENT = flinkEnv.getString("flink.app.sink.kafka.subject.event");

        // operator uid
        final String UID_KAFKA_DATA_SOURCE_NONBOT = "kafka-data-source-nonBot";
        final String UID_KAFKA_DATA_SOURCE_BOT = "kafka-data-source-Bot";
        final String UID_FILTER_VALIDATE_EVENT = "filter-map-guid-x-uid";
        final String UID_KAFKA_DATA_SINK_RNO = "kafka-data-sink-rno";
        final String UID_KAFKA_DATA_SINK_LVS = "kafka-data-sink-lvs";
        final String UID_KAFKA_DATA_SINK_SLC = "kafka-data-sink-slc";

        // operator name
        final String NAME_KAFKA_DATA_SOURCE_NONBOT = "Kafka: SojEvent Non-Bot";
        final String NAME_KAFKA_DATA_SOURCE_BOT = "Kafka: SojEvent Bot";
        final String NAME_KAFKA_DATA_SINK_RNO = "Kafka: behavior.pulsar RNO";
        final String NAME_KAFKA_DATA_SINK_LVS = "Kafka: behavior.pulsar LVS";
        final String NAME_KAFKA_DATA_SINK_SLC = "Kafka: behavior.pulsar SLC";

        // config
        final List<String> DIST_DC_LIST = flinkEnv.getList("flink.app.sink.kafka.dc");

        final String SINK_KAFKA_ENV = flinkEnv.getString(FLINK_APP_SINK_KAFKA_ENV);
        final String SINK_KAFKA_STREAM = flinkEnv.getString(FLINK_APP_SINK_KAFKA_STREAM);
        final String TOPIC_NON_BOT = flinkEnv.getString("flink.app.source.kafka.topic.non-bot");
        final String TOPIC_BOT = flinkEnv.getString("flink.app.source.kafka.topic.bot");
        final Integer PARALLELISM_SOURCE_NON_BOT = flinkEnv.getInteger("flink.app.parallelism.source.non-bot");
        final Integer PARALLELISM_SOURCE_BOT = flinkEnv.getInteger("flink.app.parallelism.source.bot");
        final Integer PARALLELISM_FLATMAP = flinkEnv.getInteger("flink.app.parallelism.flatmap");

        // kafka data source
        KafkaSource<RawSojEventWrapper> kafkaSource_nonBot =
                KafkaSource.<RawSojEventWrapper>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(TOPIC_NON_BOT)
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(KafkaRecordDeserializationSchema.of(
                                   new RawSojEventWrapperDeserializationSchema()))
                           .build();

        KafkaSource<RawSojEventWrapper> kafkaSource_bot =
                KafkaSource.<RawSojEventWrapper>builder()
                        .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                        .setGroupId(flinkEnv.getSourceKafkaGroupId())
                        .setTopics(TOPIC_BOT)
                        .setProperties(flinkEnv.getKafkaConsumerProps())
                        .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                        .setDeserializer(KafkaRecordDeserializationSchema.of(
                                new RawSojEventWrapperDeserializationSchema()))
                        .build();


        DataStream<RawSojEventWrapper> sourceDataStreamNonbot =
             executionEnvironment.fromSource(kafkaSource_nonBot, noWatermarks(),
             NAME_KAFKA_DATA_SOURCE_NONBOT)
                                 .uid(UID_KAFKA_DATA_SOURCE_NONBOT)
                                 .setParallelism(PARALLELISM_SOURCE_NON_BOT).setMaxParallelism(400).rescale();

        DataStream<RawSojEventWrapper> sourceDataStreamBot =
                executionEnvironment.fromSource(kafkaSource_bot, noWatermarks(), NAME_KAFKA_DATA_SOURCE_BOT)
                        .uid(UID_KAFKA_DATA_SOURCE_BOT)
                        .setParallelism(PARALLELISM_SOURCE_BOT).setMaxParallelism(600).rescale();



        DataStream<RawSojEventWrapper> sourceDataStream = sourceDataStreamNonbot.union(sourceDataStreamBot);

        DataStream<SojEvent> sojEventStream =
                sourceDataStream.flatMap(new GuidXUidFlatMap())
                                .name(UID_FILTER_VALIDATE_EVENT)
                                .uid(UID_FILTER_VALIDATE_EVENT)
                                .setParallelism(PARALLELISM_FLATMAP).setMaxParallelism(1200);


        // sink to rno behavior.pulsar
        if (DIST_DC_LIST.contains(RNO.toString())) {
            KafkaSink<SojEvent> rnoKafkaSink =
                    KafkaSink.<SojEvent>builder()
                             .setBootstrapServers(flinkEnv.getKafkaBrokers(SINK_KAFKA_ENV,
                                                                           SINK_KAFKA_STREAM,
                                                                           RNO.toString()))
                             .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                             .setRecordSerializer(new SojEventKafkaRecordSerializationSchema(
                                     REGISTRY_URL, SUBJECT_SOJEVENT, TOPIC_EVENT)
                             )
                             .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                             .build();


            sojEventStream.sinkTo(rnoKafkaSink)
                    .name(NAME_KAFKA_DATA_SINK_RNO)
                    .uid(UID_KAFKA_DATA_SINK_RNO)
                    .setParallelism(flinkEnv.getSinkParallelism());

        }

        // sink to lvs behavior.pulsar
        if (DIST_DC_LIST.contains(LVS.toString())) {
            KafkaSink<SojEvent> lvsKafkaSink =
                    KafkaSink.<SojEvent>builder()
                             .setBootstrapServers(flinkEnv.getKafkaBrokers(SINK_KAFKA_ENV,
                                                                           SINK_KAFKA_STREAM,
                                                                           LVS.toString()))
                             .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                             .setRecordSerializer(new SojEventKafkaRecordSerializationSchema(
                                     REGISTRY_URL, SUBJECT_SOJEVENT, TOPIC_EVENT)
                             )
                             .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                             .build();


               sojEventStream.sinkTo(lvsKafkaSink)
                              .name(NAME_KAFKA_DATA_SINK_LVS)
                              .uid(UID_KAFKA_DATA_SINK_LVS)
                              .setParallelism(flinkEnv.getSinkParallelism());

        }

        // sink to slc behavior.pulsar
        if (DIST_DC_LIST.contains(SLC.toString())) {
            KafkaSink<SojEvent> slcKafkaSink =
                    KafkaSink.<SojEvent>builder()
                             .setBootstrapServers(flinkEnv.getKafkaBrokers(SINK_KAFKA_ENV,
                                                                           SINK_KAFKA_STREAM,
                                                                           SLC.toString()))
                             .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                             .setRecordSerializer(new SojEventKafkaRecordSerializationSchema(
                                     REGISTRY_URL, SUBJECT_SOJEVENT, TOPIC_EVENT)
                             )
                             .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                             .build();

                sojEventStream.sinkTo(slcKafkaSink)
                              .name(NAME_KAFKA_DATA_SINK_SLC)
                              .uid(UID_KAFKA_DATA_SINK_SLC)
                              .setParallelism(flinkEnv.getSinkParallelism());

        }

        // Submit this job
        flinkEnv.execute(executionEnvironment);
    }

}
