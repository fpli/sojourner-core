package com.ebay.sojourner.integration.pipeline;

import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.kafka.schema.PassThroughDeserializationSchema;
import com.ebay.sojourner.flink.function.map.BytesArrayToSojEventMapFunction;
import com.ebay.sojourner.integration.function.BytesArrayToRheosEventSojEventMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_REGISTRY_URL;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class SojEventKafkaConsumerJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_SOURCE = "kafka-source";

        // operator name
        final String NAME_KAFKA_SOURCE = String.format("Kafka: SojEvent - %s", flinkEnv.getSourceKafkaStreamName());

        // config
        final String REGISTRY_URL = flinkEnv.getString(RHEOS_REGISTRY_URL);

        // kafka data source
        KafkaSource<byte[]> kafkaSource =
                KafkaSource.<byte[]>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(flinkEnv.getSourceKafkaTopics())
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(KafkaRecordDeserializationSchema.of(
                                   new PassThroughDeserializationSchema()
                           ))
                           .build();

        DataStream<byte[]> sourceStream =
                executionEnvironment.fromSource(kafkaSource, noWatermarks(), NAME_KAFKA_SOURCE)
                                    .uid(UID_KAFKA_SOURCE)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        sourceStream.map(new BytesArrayToSojEventMapFunction())
                    .name("Deserialize SojEvent directly")
                    .uid("deserialize-directly")
                    .setParallelism(flinkEnv.getSourceParallelism());

        sourceStream.map(new BytesArrayToRheosEventSojEventMapFunction(REGISTRY_URL))
                    .name("Deserialize SojEvent using rheos client sdk")
                    .uid("deserialize-rheos-client-sdk")
                    .setParallelism(flinkEnv.getSourceParallelism());


        // submit job
        flinkEnv.execute(executionEnvironment);
    }
}
