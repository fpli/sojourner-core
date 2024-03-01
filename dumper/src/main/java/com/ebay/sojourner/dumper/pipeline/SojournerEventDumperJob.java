package com.ebay.sojourner.dumper.pipeline;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.dumper.bucket.SojEventHdfsBucketAssigner;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.hdfs.OutputFileConfigUtils;
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
        final String UID_KAFKA_SOURCE_EVENT_NON_BOT = "kafka-source-event-non-bot";
        final String UID_KAFKA_SOURCE_EVENT_BOT = "kafka-source-event-bot";
        final String UID_HDFS_SINK_EVENT = "hdfs-sink-event";
        final String UID_HDFS_SINK_WATERMARK = "hdfs-sink-watermark";
        final String UID_UNIX_TS_TO_SOJ_TS = "unix-timestamp-to-soj-timestamp";
        final String UID_EXTRACT_WATERMARK = "extract-watermark";

        // operator name
        final String NAME_KAFKA_SOURCE_EVENT_NON_BOT = String.format("Kafka: SojEvent Non-Bot - %s",
                                                                     flinkEnv.getSourceKafkaStreamName());
        final String NAME_KAFKA_SOURCE_EVENT_BOT = String.format("Kafka: SojEvent Bot - %s",
                                                                 flinkEnv.getSourceKafkaStreamName());
        final String NAME_HDFS_SINK_EVENT = "HDFS Sink: SojEvent";
        final String NAME_HDFS_SINK_WATERMARK = "HDFS Sink: SojWatermark";
        final String NAME_UNIX_TS_TO_SOJ_TS = "Unix Timestamp To Soj Timestamp";
        final String NAME_EXTRACT_WATERMARK = "Extract SojWatermark";

        // config
        final String HDFS_BASE_PATH = flinkEnv.getString(FLINK_APP_SINK_HDFS_BASE_PATH);
        final String HDFS_WATERMARK_PATH = flinkEnv.getString("flink.app.sink.hdfs.watermark-path");
        final String METRIC_WATERMARK_DELAY = flinkEnv.getString("flink.app.metric.watermark-delay");

        final String TOPIC_NON_BOT = flinkEnv.getString("flink.app.source.kafka.topic.non-bot");
        final String TOPIC_BOT = flinkEnv.getString("flink.app.source.kafka.topic.bot");

        final Integer PARALLELISM_SOURCE_NON_BOT = flinkEnv.getInteger("flink.app.parallelism.source.non-bot");
        final Integer PARALLELISM_SOURCE_BOT = flinkEnv.getInteger("flink.app.parallelism.source.bot");


        // kafka data source
        KafkaSource<SojEvent> kafkaSourceNonBot =
                KafkaSource.<SojEvent>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(TOPIC_NON_BOT)
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(new SojEventDeserialization())
                           .build();

        KafkaSource<SojEvent> kafkaSourceBot =
                KafkaSource.<SojEvent>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(TOPIC_BOT)
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(new SojEventDeserialization())
                           .build();

        // watermark
        WatermarkStrategy<SojEvent> watermarkStrategy =
                WatermarkStrategy.<SojEvent>forMonotonousTimestamps()
                                 .withTimestampAssigner(new SojEventTimestampAssigner());

        DataStream<SojEvent> sourceNonBotStream =
                executionEnvironment.fromSource(kafkaSourceNonBot, watermarkStrategy, NAME_KAFKA_SOURCE_EVENT_NON_BOT)
                                    .uid(UID_KAFKA_SOURCE_EVENT_NON_BOT)
                                    .setParallelism(PARALLELISM_SOURCE_NON_BOT)
                                    .rescale();

        DataStream<SojEvent> sourceBotStream =
                executionEnvironment.fromSource(kafkaSourceBot, watermarkStrategy, NAME_KAFKA_SOURCE_EVENT_BOT)
                                    .uid(UID_KAFKA_SOURCE_EVENT_BOT)
                                    .setParallelism(PARALLELISM_SOURCE_BOT)
                                    .rescale();

        DataStream<SojEvent> sourceUnionStream = sourceNonBotStream.union(sourceBotStream);

        DataStream<SojEvent> sojEventStream =
                sourceUnionStream.map(new SojEventTimestampTransMapFunction())
                                 .name(NAME_UNIX_TS_TO_SOJ_TS)
                                 .uid(UID_UNIX_TS_TO_SOJ_TS)
                                 .setParallelism(flinkEnv.getSinkParallelism());

        // extract timestamp
        DataStream<SojWatermark> sojWatermarkStream =
                sojEventStream.process(new ExtractWatermarkProcessFunction<>(METRIC_WATERMARK_DELAY))
                              .name(NAME_EXTRACT_WATERMARK)
                              .uid(UID_EXTRACT_WATERMARK)
                              .setParallelism(flinkEnv.getSinkParallelism());

        // build hdfs sink for SojWatermark
        final FileSink<SojWatermark> sojWatermarkSink =
                FileSink.forBulkFormat(new Path(HDFS_WATERMARK_PATH),
                                       ParquetAvroWritersWithCompression.forReflectRecord(SojWatermark.class))
                        .withBucketAssigner(new SojCommonDateTimeBucketAssigner<>())
                        .withOutputFileConfig(OutputFileConfigUtils.withRandomUUID())
                        .build();

        // sink SojWatermark to hdfs
        sojWatermarkStream.sinkTo(sojWatermarkSink)
                          .name(NAME_HDFS_SINK_WATERMARK)
                          .uid(UID_HDFS_SINK_WATERMARK)
                          .setParallelism(flinkEnv.getSinkParallelism());

        // build hdfs sink for SojEvent
        final FileSink<SojEvent> sojEventSink =
                FileSink.forBulkFormat(new Path(HDFS_BASE_PATH),
                                       ParquetAvroWritersWithCompression.forSpecificRecord(SojEvent.class))
                        .withBucketAssigner(new SojEventHdfsBucketAssigner())
                        .withOutputFileConfig(OutputFileConfigUtils.withRandomUUID())
                        .build();

        // sink SojEvent to hdfs
        sojEventStream.sinkTo(sojEventSink)
                      .name(NAME_HDFS_SINK_EVENT)
                      .uid(UID_HDFS_SINK_EVENT)
                      .setParallelism(flinkEnv.getSinkParallelism());

        // submit job
        flinkEnv.execute(executionEnvironment);
    }
}
