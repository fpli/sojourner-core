package com.ebay.sojourner.dumper.pipeline;

import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.common.model.UniSession;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.hdfs.OutputFileConfigUtils;
import com.ebay.sojourner.flink.connector.hdfs.ParquetAvroWritersWithCompression;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.hdfs.bucket.UniSessionHdfsBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.schema.deserialize.UniSessionDeserialization;
import com.ebay.sojourner.flink.function.process.EmitWatermarkProcessFunction;
import com.ebay.sojourner.flink.watermark.UniSessionTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_HDFS_BASE_PATH;

public class UniSessionDumper {

    public static void main(String[] args) throws Exception {
        FlinkEnv flinkEnv = new FlinkEnv(args);
        StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

        // operator uid
        final String UID_KAFKA_SOURCE_SESSION = "kafka-source-session";
        final String UID_HDFS_SINK_SESSION = "hdfs-sink-session";
        final String UID_HDFS_SINK_WATERMARK = "hdfs-sink-watermark";
        final String UID_EXTRACT_WATERMARK = "extract-watermark";

        // operator name
        final String NAME_KAFKA_SOURCE_SESSION = String.format("Kafka: UniSession - %s",
                flinkEnv.getSourceKafkaStreamName());
        final String NAME_HDFS_SINK_SESSION = "HDFS Sink: UniSession";
        final String NAME_HDFS_SINK_WATERMARK = "HDFS Sink: Watermark";
        final String NAME_EXTRACT_WATERMARK = "Extract Watermark";

        // config
        final String HDFS_BASE_PATH = flinkEnv.getString(FLINK_APP_SINK_HDFS_BASE_PATH);
        final String HDFS_WATERMARK_PATH = flinkEnv.getString("flink.app.sink.hdfs.watermark-path");
        final String METRIC_WATERMARK_DELAY = flinkEnv.getString("flink.app.metric.watermark-delay");

        // kafka data source
        KafkaSource<UniSession> kafkaSource =
                KafkaSource.<UniSession>builder()
                        .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                        .setGroupId(flinkEnv.getSourceKafkaGroupId())
                        .setTopics(flinkEnv.getSourceKafkaTopics())
                        .setProperties(flinkEnv.getKafkaConsumerProps())
                        .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                        .setDeserializer(new UniSessionDeserialization())
                        .build();

        WatermarkStrategy<UniSession> watermarkStrategy =
                WatermarkStrategy.<UniSession>forMonotonousTimestamps()
                                 .withTimestampAssigner(new UniSessionTimestampAssigner());

        SingleOutputStreamOperator<UniSession> sourceDataStream =
                executionEnvironment.fromSource(kafkaSource, watermarkStrategy, NAME_KAFKA_SOURCE_SESSION)
                        .uid(UID_KAFKA_SOURCE_SESSION)
                        .setParallelism(flinkEnv.getSourceParallelism());


        // extract timestamp
        SingleOutputStreamOperator<SojWatermark> sojWatermarkStream =
                sourceDataStream.process(new EmitWatermarkProcessFunction<>(METRIC_WATERMARK_DELAY))
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

        // build hdfs sink for UniSession
        final FileSink<UniSession> uniSessionSink =
                FileSink.forBulkFormat(new Path(HDFS_BASE_PATH),
                                ParquetAvroWritersWithCompression.forSpecificRecord(UniSession.class))
                        .withBucketAssigner(new UniSessionHdfsBucketAssigner())
                        .withOutputFileConfig(OutputFileConfigUtils.withRandomUUID())
                        .build();

        // sink UniSession to hdfs
        sourceDataStream.sinkTo(uniSessionSink)
                .name(NAME_HDFS_SINK_SESSION)
                .uid(UID_HDFS_SINK_SESSION)
                .setParallelism(flinkEnv.getSinkParallelism());

        // submit job
        flinkEnv.execute(executionEnvironment);
    }
}
