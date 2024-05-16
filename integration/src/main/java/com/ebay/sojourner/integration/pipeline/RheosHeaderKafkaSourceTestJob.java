package com.ebay.sojourner.integration.pipeline;

import com.ebay.sojourner.common.model.RheosHeader;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.kafka.schema.deserialize.RheosHeaderRecordDeserializationSchema;
import com.ebay.sojourner.integration.function.SimpleLogFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

public class RheosHeaderKafkaSourceTestJob {
    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.local(4);

        // kafka source
        KafkaSource<RheosHeader> kafkaSource =
                KafkaSource.<RheosHeader>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(flinkEnv.getSourceKafkaTopics())
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(OffsetsInitializer.latest())
                           .setDeserializer(new RheosHeaderRecordDeserializationSchema())
                           .build();

        SingleOutputStreamOperator<RheosHeader> kafkaSourceStream =
                executionEnvironment.fromSource(kafkaSource, noWatermarks(), "Kafka Source")
                                    .name("Kafka Source")
                                    .uid("kafka-source")
                                    .setParallelism(1)
                                    .disableChaining();

        SingleOutputStreamOperator<RheosHeader> logStream = kafkaSourceStream.map(new SimpleLogFunction<>())
                                                                             .name("Log")
                                                                             .uid("log")
                                                                             .setParallelism(1);

        logStream.addSink(new DiscardingSink<>())
                 .name("Discard Sink")
                 .uid("discard-sink")
                 .setParallelism(1);

        // submit job
        flinkEnv.execute(executionEnvironment);
    }
}
