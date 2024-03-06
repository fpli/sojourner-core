package com.ebay.sojourner.dumper.pipeline;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.dumper.bucket.SessionMetricsHdfsBucketAssigner;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.hdfs.OutputFileConfigUtils;
import com.ebay.sojourner.flink.connector.hdfs.ParquetAvroWritersWithCompression;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.schema.deserialize.SessionMetricsDeserialization;
import com.ebay.sojourner.flink.function.map.SessionMetricsTimestampTransMapFunction;
import com.ebay.sojourner.flink.function.process.ExtractWatermarkProcessFunction;
import com.ebay.sojourner.flink.watermark.SessionMetricsTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_HDFS_BASE_PATH;

public class SojournerSessionMetricsDumperJob {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_SOURCE_SESSION_METRICS = "kafka-source-session-metrics";
        final String UID_HDFS_SINK_SESSION_METRICS = "hdfs-sink-session-metrics";
        final String UID_HDFS_SINK_WATERMARK = "hdfs-sink-watermark";
        final String UID_UNIX_TS_TO_SOJ_TS = "unix-timestamp-to-soj-timestamp";
        final String UID_EXTRACT_WATERMARK = "extract-watermark";

        // operator name
        final String NAME_KAFKA_SOURCE_SESSION_METRICS = String.format("Kafka: SessionMetrics - %s",
                                                                       flinkEnv.getSourceKafkaStreamName());
        final String NAME_HDFS_SINK_SESSION_METRICS = "HDFS Sink: SessionMetrics";
        final String NAME_HDFS_SINK_WATERMARK = "HDFS Sink: SojWatermark";
        final String NAME_UNIX_TS_TO_SOJ_TS = "Unix Timestamp To Soj Timestamp";
        final String NAME_EXTRACT_WATERMARK = "Extract SojWatermark";

        // config
        final String HDFS_BASE_PATH = flinkEnv.getString(FLINK_APP_SINK_HDFS_BASE_PATH);
        final String HDFS_WATERMARK_PATH = flinkEnv.getString("flink.app.sink.hdfs.watermark-path");
        final String METRIC_WATERMARK_DELAY = flinkEnv.getString("flink.app.metric.watermark-delay");


        // kafka data source
        KafkaSource<SessionMetrics> kafkaSource =
                KafkaSource.<SessionMetrics>builder()
                           .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                           .setGroupId(flinkEnv.getSourceKafkaGroupId())
                           .setTopics(flinkEnv.getSourceKafkaTopics())
                           .setProperties(flinkEnv.getKafkaConsumerProps())
                           .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                           .setDeserializer(new SessionMetricsDeserialization())
                           .build();

        // watermark
        WatermarkStrategy<SessionMetrics> watermarkStrategy =
                WatermarkStrategy.<SessionMetrics>forMonotonousTimestamps()
                                 .withTimestampAssigner(new SessionMetricsTimestampAssigner());

        SingleOutputStreamOperator<SessionMetrics> sourceDataStream =
                executionEnvironment.fromSource(kafkaSource, watermarkStrategy, NAME_KAFKA_SOURCE_SESSION_METRICS)
                                    .uid(UID_KAFKA_SOURCE_SESSION_METRICS)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        // unix timestamp to sojourner timestamp
        SingleOutputStreamOperator<SessionMetrics> sessionMetricsStream =
                sourceDataStream.map(new SessionMetricsTimestampTransMapFunction())
                                .name(NAME_UNIX_TS_TO_SOJ_TS)
                                .uid(UID_UNIX_TS_TO_SOJ_TS)
                                .setParallelism(flinkEnv.getSourceParallelism());

        // extract timestamp
        SingleOutputStreamOperator<SojWatermark> sojWatermarkStream =
                sessionMetricsStream.process(new ExtractWatermarkProcessFunction<>(METRIC_WATERMARK_DELAY))
                                    .name(NAME_EXTRACT_WATERMARK)
                                    .uid(UID_EXTRACT_WATERMARK)
                                    .setParallelism(flinkEnv.getSourceParallelism());

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

        // build hdfs sink for SessionMetrics
        final FileSink<SessionMetrics> sessionMetricsSink =
                FileSink.forBulkFormat(new Path(HDFS_BASE_PATH),
                                       ParquetAvroWritersWithCompression.forSpecificRecord(SessionMetrics.class))
                        .withBucketAssigner(new SessionMetricsHdfsBucketAssigner())
                        .withOutputFileConfig(OutputFileConfigUtils.withRandomUUID())
                        .build();

        // sink SessionMetrics to hdfs
        sessionMetricsStream.sinkTo(sessionMetricsSink)
                            .name(NAME_HDFS_SINK_SESSION_METRICS)
                            .uid(UID_HDFS_SINK_SESSION_METRICS)
                            .setParallelism(flinkEnv.getSinkParallelism());

        // submit job
        flinkEnv.execute(executionEnvironment);
    }
}
