package com.ebay.sojourner.dumper.pipeline;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojWatermark;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.hdfs.HdfsConnectorFactory;
import com.ebay.sojourner.flink.connector.hdfs.SojEventDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.SojSerializableTimestampAssigner;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.PassThroughDeserializationSchema;
import com.ebay.sojourner.flink.function.BinaryToSojEventMapFunction;
import com.ebay.sojourner.flink.function.ExtractWatermarkProcessFunction;
import com.ebay.sojourner.flink.function.SojEventTimestampTransMapFunction;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojournerEventDumperJob {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    String dc = getString(Property.FLINK_APP_SOURCE_DC);

    // rescaled kafka source
    SourceDataStreamBuilder<byte[]> dataStreamBuilder =
        new SourceDataStreamBuilder<>(executionEnvironment);

    DataStream<byte[]> rescaledByteEventDataStream = dataStreamBuilder
        .dc(DataCenter.of(dc))
        .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
        .uid(getString(Property.SOURCE_UID))
        .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
        .buildRescaled(new PassThroughDeserializationSchema());

    // byte to sojevent
    DataStream<SojEvent> sojEventDataStream = rescaledByteEventDataStream
        .map(new BinaryToSojEventMapFunction())
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.PASS_THROUGH_OPERATOR_NAME))
        .uid(getString(Property.PASS_THROUGH_UID));

    // assgin watermark
    DataStream<SojEvent> assignedWatermarkSojEventDataStream = sojEventDataStream
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<SojEvent>forBoundedOutOfOrderness(Duration.ofMinutes(
                    FlinkEnvUtils.getInteger(Property.FLINK_APP_SOURCE_OUT_OF_ORDERLESS_IN_MIN)))
                .withTimestampAssigner(new SojSerializableTimestampAssigner<>()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.ASSIGN_WATERMARK_OPERATOR_NAME))
        .uid(getString(Property.ASSIGN_WATERMARK_UID));

    // unix timestamp to sojourner timestamp
    DataStream<SojEvent> finalSojEventDataStream = assignedWatermarkSojEventDataStream
        .map(new SojEventTimestampTransMapFunction())
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name("Unix Timestamp To Soj Timestamp")
        .uid("unix-timestamp-to-soj-timestamp");

    // extract timestamp
    DataStream<SojWatermark> sojEventWatermarkStream = finalSojEventDataStream
        .process(new ExtractWatermarkProcessFunction<>(
            getString(Property.FLINK_APP_METRIC_NAME)))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.TIMESTAMP_EXTRACT_OPERATOR_NAME))
        .uid(getString(Property.TIMESTAMP_EXTRACT_UID));

    // sink timestamp to hdfs
    sojEventWatermarkStream
        .addSink(HdfsConnectorFactory.createWithParquet(
            getString(Property.FLINK_APP_SINK_HDFS_WATERMARK_PATH), SojWatermark.class,
            new SojCommonDateTimeBucketAssigner<>()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.SINK_OPERATOR_NAME_WATERMARK))
        .uid(getString(Property.SINK_UID_WATERMARK));

    // hdfs sink
    finalSojEventDataStream
        .addSink(HdfsConnectorFactory.createWithParquet(
            getString(Property.FLINK_APP_SINK_HDFS_PATH), SojEvent.class,
            new SojEventDateTimeBucketAssigner()))
        .setParallelism(getInteger(Property.SINK_HDFS_PARALLELISM))
        .name(getString(Property.SINK_OPERATOR_NAME_EVENT))
        .uid(getString(Property.SINK_UID_EVENT));

    // submit job
    FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
  }
}
