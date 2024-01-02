package com.ebay.sojourner.dumper.pipeline;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SINK;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SOURCE;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_HDFS_BASE_PATH;

import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.dumper.bucket.SojSessionHdfsBucketAssigner;
import com.ebay.sojourner.flink.common.FlinkEnv;
import com.ebay.sojourner.flink.connector.hdfs.ParquetAvroWritersWithCompression;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.schema.SojSessionDeserialization;
import com.ebay.sojourner.flink.function.process.ExtractWatermarkProcessFunction;
import com.ebay.sojourner.flink.function.map.SojSessionTimestampTransMapFunction;
import com.ebay.sojourner.flink.watermark.SojSessionTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojournerSessionDumperJob {

  public static void main(String[] args) throws Exception {

    FlinkEnv flinkEnv = new FlinkEnv(args);
    StreamExecutionEnvironment executionEnvironment = flinkEnv.init();

    // operator uid
    final String UID_KAFKA_DATA_SOURCE = "kafka-data-source";
    final String UID_HDFS_DATA_SINK = "hdfs-data-sink";
    final String UID_UNIX_TS_TO_SOJ_TS = "unix-timestamp-to-soj-timestamp";

    // operator name
    final String NAME_KAFKA_DATA_SOURCE = "Kafka: SojSession";
    final String NAME_HDFS_DATA_SINK = "HDFS: SojSession";
    final String NAME_UNIX_TS_TO_SOJ_TS = "Unix Timestamp To Soj Timestamp";

    // config
    final String BASE_PATH = flinkEnv.getString(FLINK_APP_SINK_HDFS_BASE_PATH);
    final String WATERMARK_PATH = flinkEnv.getString("flink.app.sink.hdfs.watermark-path");
    final String WATERMARK_DELAY_METRIC = flinkEnv.getString("flink.app.metric.watermark-delay");

    // kafka data source
    KafkaSource<SojSession> kafkaSource =
        KafkaSource.<SojSession>builder()
                   .setBootstrapServers(flinkEnv.getSourceKafkaBrokers())
                   .setGroupId(flinkEnv.getSourceKafkaGroupId())
                   .setTopics(flinkEnv.getSourceKafkaTopics())
                   .setProperties(flinkEnv.getKafkaConsumerProps())
                   .setStartingOffsets(flinkEnv.getSourceKafkaStartingOffsets())
                   .setDeserializer(new SojSessionDeserialization())
                   .build();

    // watermark
    WatermarkStrategy<SojSession> watermarkStrategy =
        WatermarkStrategy.<SojSession>forMonotonousTimestamps()
                         .withTimestampAssigner(new SojSessionTimestampAssigner());

    SingleOutputStreamOperator<SojSession> sourceDataStream =
        executionEnvironment.fromSource(kafkaSource, watermarkStrategy, NAME_KAFKA_DATA_SOURCE)
                            .uid(UID_KAFKA_DATA_SOURCE)
                            .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

    SingleOutputStreamOperator<SojSession> sojSessionStream =
        sourceDataStream.map(new SojSessionTimestampTransMapFunction())
                        .name(NAME_UNIX_TS_TO_SOJ_TS)
                        .uid(UID_UNIX_TS_TO_SOJ_TS)
                        .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

    // extract timestamp
    SingleOutputStreamOperator<SojWatermark> sojWatermarkStream =
        sojSessionStream.process(new ExtractWatermarkProcessFunction<>(WATERMARK_DELAY_METRIC))
                        .name("Extract SojWatermark")
                        .uid("extract-watermark")
                        .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SOURCE));

    // sink for SojWatermark
    final FileSink<SojWatermark> sojWatermarkSink =
        FileSink.forBulkFormat(
                    new Path(WATERMARK_PATH),
                    ParquetAvroWritersWithCompression.forReflectRecord(SojWatermark.class)
                )
                .withBucketAssigner(new SojCommonDateTimeBucketAssigner<>())
                .build();

    // sink SojWatermark to hdfs
    sojWatermarkStream.sinkTo(sojWatermarkSink)
                      .name("Watermark Sink")
                      .uid("watermark-sink")
                      .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SINK));

    // sink for SojSession
    final FileSink<SojSession> sojSessionSink =
        FileSink.forBulkFormat(
                    new Path(BASE_PATH),
                    ParquetAvroWritersWithCompression.forSpecificRecord(SojSession.class)
                )
                .withBucketAssigner(new SojSessionHdfsBucketAssigner())
                .build();

    // sink SojSession to hdfs
    sojSessionStream.sinkTo(sojSessionSink)
                    .name(NAME_HDFS_DATA_SINK)
                    .uid(UID_HDFS_DATA_SINK)
                    .setParallelism(flinkEnv.getInteger(FLINK_APP_PARALLELISM_SINK));

    // submit job
    flinkEnv.execute(executionEnvironment);
  }
}
