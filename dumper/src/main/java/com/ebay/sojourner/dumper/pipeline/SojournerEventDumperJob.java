package com.ebay.sojourner.dumper.pipeline;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.dumper.bucket.SojEventHdfsBucketAssigner;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.hdfs.ParquetAvroWritersWithCompression;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.schema.deserialize.SojEventDeserialization;
import com.ebay.sojourner.flink.function.map.SojEventTimestampTransMapFunction;
import com.ebay.sojourner.flink.function.process.ExtractWatermarkProcessFunction;
import com.ebay.sojourner.flink.watermark.SojEventTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_HDFS_BASE_PATH;

public class SojournerEventDumperJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_DATA_SOURCE_NON_BOT = "kafka-data-source-non-bot";
        final String UID_KAFKA_DATA_SOURCE_BOT = "kafka-data-source-bot";
        final String UID_HDFS_DATA_SINK = "hdfs-data-sink";
        final String UID_UNIX_TS_TO_SOJ_TS = "unix-timestamp-to-soj-timestamp";

        // operator name
        final String NAME_KAFKA_DATA_SOURCE_NON_BOT = String.format("Kafka: SojEvent Non-Bot - %s",
                                                                    flinkEnv.getSourceKafkaStreamName());
        final String NAME_KAFKA_DATA_SOURCE_BOT = String.format("Kafka: SojEvent Bot - %s",
                                                                flinkEnv.getSourceKafkaStreamName());
        final String NAME_HDFS_DATA_SINK = "HDFS: SojEvent";
        final String NAME_UNIX_TS_TO_SOJ_TS = "Unix Timestamp To Soj Timestamp";

        // config
        final String BASE_PATH = flinkEnv.getString(FLINK_APP_SINK_HDFS_BASE_PATH);
        final String WATERMARK_PATH = flinkEnv.getString("flink.app.sink.hdfs.watermark-path");
        final String WATERMARK_DELAY_METRIC = flinkEnv.getString("flink.app.metric.watermark-delay");

        final String NON_BOT_TOPIC = flinkEnv.getString("flink.app.source.kafka.topic.non-bot");
        final String BOT_TOPIC = flinkEnv.getString("flink.app.source.kafka.topic.bot");

        final Integer SOURCE_BOT_PARALLELISM = flinkEnv.getInteger("flink.app.parallelism.source.bot");
        final Integer SOURCE_NON_BOT_PARALLELISM = flinkEnv.getInteger("flink.app.parallelism.source.non-bot");


        // kafka data source
        KafkaSource<SojEvent> kafkaSourceNonBot =
                KafkaSource.<SojEvent>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(NON_BOT_TOPIC)
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(new SojEventDeserialization())
                           .build();

        KafkaSource<SojEvent> kafkaSourceBot =
                KafkaSource.<SojEvent>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(BOT_TOPIC)
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(new SojEventDeserialization())
                           .build();

        // watermark
        WatermarkStrategy<SojEvent> watermarkStrategy =
                WatermarkStrategy.<SojEvent>forMonotonousTimestamps()
                                 .withTimestampAssigner(new SojEventTimestampAssigner());

        DataStream<SojEvent> sourceNonBotStream =
                executionEnvironment.fromSource(kafkaSourceNonBot, watermarkStrategy, NAME_KAFKA_DATA_SOURCE_NON_BOT)
                                    .uid(UID_KAFKA_DATA_SOURCE_NON_BOT)
                                    .setParallelism(SOURCE_NON_BOT_PARALLELISM)
                                    .rescale();

        DataStream<SojEvent> sourceBotStream =
                executionEnvironment.fromSource(kafkaSourceBot, watermarkStrategy, NAME_KAFKA_DATA_SOURCE_BOT)
                                    .uid(UID_KAFKA_DATA_SOURCE_BOT)
                                    .setParallelism(SOURCE_BOT_PARALLELISM)
                                    .rescale();

        DataStream<SojEvent> sourceUnionStream = sourceNonBotStream.union(sourceBotStream);

        DataStream<SojEvent> sojEventStream =
                sourceUnionStream.map(new SojEventTimestampTransMapFunction())
                                 .name(NAME_UNIX_TS_TO_SOJ_TS)
                                 .uid(UID_UNIX_TS_TO_SOJ_TS)
                                 .setParallelism(flinkEnv.getSinkParallelism());

        // extract timestamp
        DataStream<SojWatermark> sojWatermarkStream =
                sojEventStream.process(new ExtractWatermarkProcessFunction<>(WATERMARK_DELAY_METRIC))
                              .name("Extract SojWatermark")
                              .uid("extract-watermark")
                              .setParallelism(flinkEnv.getSinkParallelism());

        // sink for SojWatermark
        final FileSink<SojWatermark> sojWatermarkSink =
                FileSink.forBulkFormat(new Path(WATERMARK_PATH),
                                       ParquetAvroWritersWithCompression.forReflectRecord(SojWatermark.class))
                        .withBucketAssigner(new SojCommonDateTimeBucketAssigner<>())
                        .build();

        // sink SojWatermark to hdfs
        sojWatermarkStream.sinkTo(sojWatermarkSink)
                          .name("Watermark Sink")
                          .uid("watermark-sink")
                          .setParallelism(flinkEnv.getSinkParallelism());

        // sink for SojEvent
        final FileSink<SojEvent> sojEventSink =
                FileSink.forBulkFormat(new Path(BASE_PATH),
                                       ParquetAvroWritersWithCompression.forSpecificRecord(SojEvent.class))
                        .withBucketAssigner(new SojEventHdfsBucketAssigner())
                        .build();

        // sink SojEvent to hdfs
        sojEventStream.sinkTo(sojEventSink)
                      .name(NAME_HDFS_DATA_SINK)
                      .uid(UID_HDFS_DATA_SINK)
                      .setParallelism(flinkEnv.getSinkParallelism());

        // submit job
        flinkEnv.execute(executionEnvironment);
    }
}
