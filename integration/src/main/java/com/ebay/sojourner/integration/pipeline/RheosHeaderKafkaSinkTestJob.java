package com.ebay.sojourner.integration.pipeline;

import com.ebay.sojourner.common.model.RheosHeader;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.RheosHeaderKeySerializerSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.serialize.RheosHeaderValueSerializerSchema;
import com.ebay.sojourner.integration.function.SimpleLogFunction;
import com.ebay.sojourner.integration.source.LocalSourceFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RheosHeaderKafkaSinkTestJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        SingleOutputStreamOperator<RheosHeader> localSource = executionEnvironment.addSource(new LocalSourceFunction())
                                                                                  .name("Local Source")
                                                                                  .uid("local-source")
                                                                                  .setParallelism(1)
                                                                                  .disableChaining();

        SingleOutputStreamOperator<RheosHeader> logStream = localSource.map(new SimpleLogFunction<>())
                                                                       .name("Log")
                                                                       .uid("log")
                                                                       .setParallelism(1);

        // kafka sink
        KafkaSink<RheosHeader> kafkaSink =
                KafkaSink.<RheosHeader>builder()
                         .setBootstrapServers(flinkEnv.getSinkKafkaBrokers())
                         .setKafkaProducerConfig(flinkEnv.getKafkaProducerProps())
                         .setRecordSerializer(
                                 KafkaRecordSerializationSchema.<RheosHeader>builder()
                                                               .setTopic(flinkEnv.getSinkKafkaTopic())
                                                               .setKeySerializationSchema(
                                                                       new RheosHeaderKeySerializerSchema())
                                                               .setValueSerializationSchema(
                                                                       new RheosHeaderValueSerializerSchema())
                                                               .build()
                         )
                         .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                         .build();

        logStream.sinkTo(kafkaSink)
                 .name("Sink to Kafka")
                 .uid("kafka-sink")
                 .setParallelism(1);

        // submit job
        flinkEnv.execute(executionEnvironment);
    }

}
