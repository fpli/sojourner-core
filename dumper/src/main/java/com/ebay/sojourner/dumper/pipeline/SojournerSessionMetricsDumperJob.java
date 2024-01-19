package com.ebay.sojourner.dumper.pipeline;

import com.ebay.sojourner.common.model.SessionMetrics;
import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.common.OutputTagConstants;
import com.ebay.sojourner.flink.connector.hdfs.HdfsConnectorFactory;
import com.ebay.sojourner.flink.connector.hdfs.SessionMetricsDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.SojSerializableTimestampAssigner;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.PassThroughDeserializationSchema;
import com.ebay.sojourner.flink.function.BinaryToSessionMetricsMapFunction;
import com.ebay.sojourner.flink.function.SessionMetricsTimestampTransMapFunction;
import com.ebay.sojourner.flink.function.SplitMetricsProcessFunction;
import com.ebay.sojourner.flink.function.process.ExtractWatermarkProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

public class SojournerSessionMetricsDumperJob {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    String dc = getString(Property.FLINK_APP_SOURCE_DC);

    // rescaled kafka source
    SourceDataStreamBuilder<byte[]> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<byte[]> rescaledByteMetricsDataStream = dataStreamBuilder
        .dc(DataCenter.of(dc))
        .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
        .uid(getString(Property.SOURCE_UID))
        .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
        .buildRescaled(new PassThroughDeserializationSchema());

    // byte to sessionMetrics
    DataStream<SessionMetrics> sessionMetricsDataStream = rescaledByteMetricsDataStream
        .map(new BinaryToSessionMetricsMapFunction())
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.PASS_THROUGH_OPERATOR_NAME))
        .uid(getString(Property.PASS_THROUGH_UID));

    // assgin watermark
    DataStream<SessionMetrics> assignedWatermarkSessionMetricsDataStream = sessionMetricsDataStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<SessionMetrics>forBoundedOutOfOrderness(Duration.ofMinutes(
                    FlinkEnvUtils.getInteger(Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN)))
                .withTimestampAssigner(new SojSerializableTimestampAssigner<>()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.ASSIGN_WATERMARK_OPERATOR_NAME))
        .uid(getString(Property.ASSIGN_WATERMARK_UID));

    // unix timestamp to sojourner timestamp
    DataStream<SessionMetrics> finalSessionMetricsDataStream = assignedWatermarkSessionMetricsDataStream
        .map(new SessionMetricsTimestampTransMapFunction())
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name("Unix Timestamp To Soj Timestamp")
        .uid("unix-timestamp-to-soj-timestamp");

    // extract timestamp
    DataStream<SojWatermark> sessionMetricsWatermarkStream = finalSessionMetricsDataStream
        .process(new ExtractWatermarkProcessFunction<>(
            getString(Property.FLINK_APP_METRIC_NAME)))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.TIMESTAMP_EXTRACT_OPERATOR_NAME))
        .uid(getString(Property.TIMESTAMP_EXTRACT_UID));

    // sink timestamp to hdfs
    sessionMetricsWatermarkStream
        .addSink(HdfsConnectorFactory.createWithParquet(
            getString(Property.FLINK_APP_SINK_HDFS_WATERMARK_PATH), SojWatermark.class,
            new SojCommonDateTimeBucketAssigner<>()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.SINK_OPERATOR_NAME_WATERMARK))
        .uid(getString(Property.SINK_UID_WATERMARK));

    SingleOutputStreamOperator<SessionMetrics> sameDaySessionMetricsStream =
        finalSessionMetricsDataStream
            .process(new SplitMetricsProcessFunction(OutputTagConstants.crossDaySessionMetricsOutputTag,
                OutputTagConstants.openSessionMetricsOutputTag))
            .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
            .name(getString(Property.SESSION_METRICS_SPLIT_OPERATOR_NAME))
            .uid(getString(Property.SESSION_METRICS_SPLIT_UID));

    DataStream<SessionMetrics> crossDaySessionMetricsStream = sameDaySessionMetricsStream
        .getSideOutput(OutputTagConstants.crossDaySessionMetricsOutputTag);

    DataStream<SessionMetrics> openSessionMetricsStream = sameDaySessionMetricsStream
        .getSideOutput(OutputTagConstants.openSessionMetricsOutputTag);

    // same day session metrics hdfs sink
    sameDaySessionMetricsStream
        .addSink(HdfsConnectorFactory.createWithParquet(
            getString(Property.FLINK_APP_SINK_HDFS_SAME_DAY_SESSION_METRICS_PATH), SessionMetrics.class,
            new SessionMetricsDateTimeBucketAssigner()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.SINK_OPERATOR_NAME_SESSION_METRICS_SAME_DAY))
        .uid(getString(Property.SINK_UID_SESSION_METRICS_SAME_DAY));

    // cross day session metrics hdfs sink
    crossDaySessionMetricsStream
        .addSink(HdfsConnectorFactory.createWithParquet(
            getString(Property.FLINK_APP_SINK_HDFS_CROSS_DAY_SESSION_METRICS_PATH), SessionMetrics.class,
            new SessionMetricsDateTimeBucketAssigner()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.SINK_OPERATOR_NAME_SESSION_METRICS_CROSS_DAY))
        .uid(getString(Property.SINK_UID_SESSION_METRICS_CROSS_DAY));

    // open session metrics hdfs sink
    openSessionMetricsStream
        .addSink(HdfsConnectorFactory.createWithParquet(
            getString(Property.FLINK_APP_SINK_HDFS_OPEN_SESSION_METRICS_PATH), SessionMetrics.class,
            new SessionMetricsDateTimeBucketAssigner()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.SINK_OPERATOR_NAME_SESSION_METRICS_OPEN))
        .uid(getString(Property.SINK_UID_SESSION_METRICS_OPEN));

    // submit job
    FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
  }
}
