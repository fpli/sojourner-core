package com.ebay.sojourner.dumper.pipeline;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.dumper.bucket.SessionMetricsHdfsBucketAssigner;
import com.ebay.sojourner.flink.common.FlinkEnv;
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

public class SojournerSessionMetricsDumperJob117 {

    public static void main(String[] args) throws Exception {

        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_DATA_SOURCE = "kafka-data-source";
        final String UID_HDFS_DATA_SINK = "hdfs-data-sink";
        final String UID_UNIX_TS_TO_SOJ_TS = "unix-timestamp-to-soj-timestamp";

        // operator name
        final String NAME_KAFKA_DATA_SOURCE = "Kafka: SessionMetrics";
        final String NAME_HDFS_DATA_SINK = "HDFS: SessionMetrics";
        final String NAME_UNIX_TS_TO_SOJ_TS = "Unix Timestamp To Soj Timestamp";

        // config
        final String BASE_PATH = flinkEnv.getString(FLINK_APP_SINK_HDFS_BASE_PATH);
        final String WATERMARK_PATH = flinkEnv.getString("flink.app.sink.hdfs.watermark-path");
        final String WATERMARK_DELAY_METRIC = flinkEnv.getString("flink.app.metric.watermark-delay");


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
                executionEnvironment.fromSource(kafkaSource, watermarkStrategy, NAME_KAFKA_DATA_SOURCE)
                                    .uid(UID_KAFKA_DATA_SOURCE)
                                    .setParallelism(flinkEnv.getSourceParallelism());

        // unix timestamp to sojourner timestamp
        SingleOutputStreamOperator<SessionMetrics> sessionMetricsStream =
                sourceDataStream.map(new SessionMetricsTimestampTransMapFunction())
                                .name(NAME_UNIX_TS_TO_SOJ_TS)
                                .uid(UID_UNIX_TS_TO_SOJ_TS)
                                .setParallelism(flinkEnv.getSourceParallelism());

        // extract timestamp
        SingleOutputStreamOperator<SojWatermark> sojWatermarkStream =
                sessionMetricsStream.process(new ExtractWatermarkProcessFunction<>(WATERMARK_DELAY_METRIC))
                                    .name("Extract SojWatermark")
                                    .uid("extract-watermark")
                                    .setParallelism(flinkEnv.getSourceParallelism());

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

        // sink for SessionMetrics
        final FileSink<SessionMetrics> sessionMetricsSink =
                FileSink.forBulkFormat(new Path(BASE_PATH),
                                       ParquetAvroWritersWithCompression.forSpecificRecord(SessionMetrics.class))
                        .withBucketAssigner(new SessionMetricsHdfsBucketAssigner())
                        .build();

        // sink SessionMetrics to hdfs
        sessionMetricsStream.sinkTo(sessionMetricsSink)
                            .name(NAME_HDFS_DATA_SINK)
                            .uid(UID_HDFS_DATA_SINK)
                            .setParallelism(flinkEnv.getSinkParallelism());

        // submit job
        flinkEnv.execute(executionEnvironment);
    }
}
