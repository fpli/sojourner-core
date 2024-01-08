package com.ebay.sojourner.distributor.pipeline;

import com.ebay.sojourner.common.model.SimpleDistSojEventWrapper;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.distributor.function.AddTagMapFunction;
import com.ebay.sojourner.distributor.function.CFlagFilterFunction;
import com.ebay.sojourner.distributor.function.SimpleDistSojEventWrapperProcessFunction;
import com.ebay.sojourner.distributor.function.SojEventDistByColoProcessFunction;
import com.ebay.sojourner.distributor.schema.bullseye.BullseyeSojEventDeserializationSchema;
import com.ebay.sojourner.distributor.schema.bullseye.SimpleDistSojEventWrapperKeySerializerSchema;
import com.ebay.sojourner.distributor.schema.bullseye.SimpleDistSojEventWrapperValueSerializerSchema;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_KAFKA_ENV;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_KAFKA_STREAM;
import static com.ebay.sojourner.flink.common.DataCenter.LVS;
import static com.ebay.sojourner.flink.common.DataCenter.RNO;
import static com.ebay.sojourner.flink.common.DataCenter.SLC;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class SojEventSimpleDistJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_DATA_SOURCE = "kafka-data-source";
        final String UID_FILTER_CFLAG = "filter-cflg";
        final String UID_MAP_ADD_TAG = "map-add-tag";
        final String UID_MAP_RAWSOJEVENTWRAPPER = "sojevent-to-simpledistsojeventwrapper";
        final String UID_DIST = "dist";
        final String UID_KAFKA_DATA_SINK_RNO = "kafka-data-sink-rno";
        final String UID_KAFKA_DATA_SINK_LVS = "kafka-data-sink-lvs";
        final String UID_KAFKA_DATA_SINK_SLC = "kafka-data-sink-slc";

        // operator name
        final String NAME_KAFKA_DATA_SOURCE = "Kafka: SojEvent";
        final String NAME_FILTER_CFLAG = "Filter SojEvent by cflag";
        final String NAME_MAP_ADD_TAG = "Add legacy tag to payload";
        final String NAME_MAP_RAWSOJEVENTWRAPPER = "SojEvent To SimpleDistSojEventWrapper";
        final String NAME_DIST = "SojEvent Distribution";
        final String NAME_KAFKA_DATA_SINK_RNO = "Kafka: Behavior.raw RNO";
        final String NAME_KAFKA_DATA_SINK_LVS = "Kafka: Behavior.raw LVS";
        final String NAME_KAFKA_DATA_SINK_SLC = "Kafka: Behavior.raw SLC";

        // config
        final Long LARGE_MESSAGE_MAX_BYTES = flinkEnv.getLong("flink.app.filter.large-message.max-bytes");
        final List<String> DIST_KEY_LIST = flinkEnv.getStringList("flink.app.dist.key-list", ",");
        final List<String> DIST_TOPIC_CONFIG_LIST = flinkEnv.getList("flink.app.dist.topic-config");
        final List<String> DIST_HASH_KEY = flinkEnv.getList("flink.app.dist.hash-key");
        final List<String> DIST_DC_LIST = flinkEnv.getList("flink.app.dist.dc");

        final String SINK_KAFKA_ENV = flinkEnv.getString(FLINK_APP_SINK_KAFKA_ENV);
        final String SINK_KAFKA_STREAM = flinkEnv.getString(FLINK_APP_SINK_KAFKA_STREAM);

        // kafka data source
        KafkaSource<SojEvent> kafkaSource =
                KafkaSource.<SojEvent>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(flinkEnv.getSourceKafkaTopics())
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(KafkaRecordDeserializationSchema.of(
                                   new BullseyeSojEventDeserializationSchema()
                           ))
                           .build();

        SingleOutputStreamOperator<SojEvent> sourceDataStream =
                executionEnvironment.fromSource(kafkaSource, noWatermarks(), NAME_KAFKA_DATA_SOURCE)
                                    .uid(UID_KAFKA_DATA_SOURCE)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        SingleOutputStreamOperator<SojEvent> enrichedSojEventStream =
                sourceDataStream.filter(new CFlagFilterFunction())
                                .name(NAME_FILTER_CFLAG)
                                .uid(UID_FILTER_CFLAG)
                                .setParallelism(flinkEnv.getSourceParallelism())
                                .map(new AddTagMapFunction())
                                .name(NAME_MAP_ADD_TAG)
                                .uid(UID_MAP_ADD_TAG)
                                .setParallelism(flinkEnv.getSourceParallelism());

        SingleOutputStreamOperator<SimpleDistSojEventWrapper> simpleDistSojEventWrapperStream =
                enrichedSojEventStream.process(new SimpleDistSojEventWrapperProcessFunction(
                                              DIST_KEY_LIST,
                                              DIST_TOPIC_CONFIG_LIST,
                                              LARGE_MESSAGE_MAX_BYTES))
                                      .name(NAME_MAP_RAWSOJEVENTWRAPPER)
                                      .uid(UID_MAP_RAWSOJEVENTWRAPPER)
                                      .setParallelism(flinkEnv.getSourceParallelism());

        SingleOutputStreamOperator<SimpleDistSojEventWrapper> sojEventDistStream =
                simpleDistSojEventWrapperStream.process(new SojEventDistByColoProcessFunction(DIST_HASH_KEY, DIST_DC_LIST))
                                               .name(NAME_DIST)
                                               .uid(UID_DIST)
                                               .setParallelism(flinkEnv.getSourceParallelism());

        // sink to rno behavior.raw
        if (DIST_DC_LIST.contains(RNO.toString())) {
            KafkaSink<SimpleDistSojEventWrapper> rnoKafkaSink =
                    KafkaSink.<SimpleDistSojEventWrapper>builder()
                             .setBootstrapServers(flinkEnv.getKafkaBrokers(SINK_KAFKA_ENV, SINK_KAFKA_STREAM, RNO.toString()))
                             .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                             .setRecordSerializer(
                                     KafkaRecordSerializationSchema.<SimpleDistSojEventWrapper>builder()
                                                                   .setTopicSelector(SimpleDistSojEventWrapper::getTopic)
                                                                   .setKeySerializationSchema(
                                                                           new SimpleDistSojEventWrapperKeySerializerSchema())
                                                                   .setValueSerializationSchema(
                                                                           new SimpleDistSojEventWrapperValueSerializerSchema())
                                                                   .build()
                             )
                             .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                             .build();

            sojEventDistStream.getSideOutput(OutputTagConstants.rnoDistOutputTag)
                              .sinkTo(rnoKafkaSink)
                              .name(NAME_KAFKA_DATA_SINK_RNO)
                              .uid(UID_KAFKA_DATA_SINK_RNO)
                              .setParallelism(flinkEnv.getSinkParallelism());

        }

        // sink to lvs behavior.raw
        if (DIST_DC_LIST.contains(LVS.toString())) {
            KafkaSink<SimpleDistSojEventWrapper> lvsKafkaSink =
                    KafkaSink.<SimpleDistSojEventWrapper>builder()
                             .setBootstrapServers(flinkEnv.getKafkaBrokers(SINK_KAFKA_ENV, SINK_KAFKA_STREAM, LVS.toString()))
                             .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                             .setRecordSerializer(
                                     KafkaRecordSerializationSchema.<SimpleDistSojEventWrapper>builder()
                                                                   .setTopicSelector(SimpleDistSojEventWrapper::getTopic)
                                                                   .setKeySerializationSchema(
                                                                           new SimpleDistSojEventWrapperKeySerializerSchema())
                                                                   .setValueSerializationSchema(
                                                                           new SimpleDistSojEventWrapperValueSerializerSchema())
                                                                   .build()
                             )
                             .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                             .build();

            sojEventDistStream.getSideOutput(OutputTagConstants.lvsDistOutputTag)
                              .sinkTo(lvsKafkaSink)
                              .name(NAME_KAFKA_DATA_SINK_LVS)
                              .uid(UID_KAFKA_DATA_SINK_LVS)
                              .setParallelism(flinkEnv.getSinkParallelism());

        }

        // sink to slc behavior.raw
        if (DIST_DC_LIST.contains(SLC.toString())) {
            KafkaSink<SimpleDistSojEventWrapper> slcKafkaSink =
                    KafkaSink.<SimpleDistSojEventWrapper>builder()
                             .setBootstrapServers(flinkEnv.getKafkaBrokers(SINK_KAFKA_ENV, SINK_KAFKA_STREAM, SLC.toString()))
                             .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                             .setRecordSerializer(
                                     KafkaRecordSerializationSchema.<SimpleDistSojEventWrapper>builder()
                                                                   .setTopicSelector(SimpleDistSojEventWrapper::getTopic)
                                                                   .setKeySerializationSchema(
                                                                           new SimpleDistSojEventWrapperKeySerializerSchema())
                                                                   .setValueSerializationSchema(
                                                                           new SimpleDistSojEventWrapperValueSerializerSchema())
                                                                   .build()
                             )
                             .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                             .build();

            sojEventDistStream.getSideOutput(OutputTagConstants.slcDistOutputTag)
                              .sinkTo(slcKafkaSink)
                              .name(NAME_KAFKA_DATA_SINK_SLC)
                              .uid(UID_KAFKA_DATA_SINK_SLC)
                              .setParallelism(flinkEnv.getSinkParallelism());

        }

        // Submit this job
        flinkEnv.execute(executionEnvironment);
    }

}
