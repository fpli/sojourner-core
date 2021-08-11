package com.ebay.sojourner.integration.pipeline;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.hdfs.HdfsConnectorFactory;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.AvroKafkaDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojCustomizedTopicDumperJob {
  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    String hdfsPath = getString(Property.FLINK_APP_SINK_HDFS_PATH);
    int sinkParallelNum = FlinkEnvUtils.getInteger(Property.SINK_HDFS_PARALLELISM);
    String dc = getString(Property.FLINK_APP_SOURCE_DC);

      SourceDataStreamBuilder<SojEvent> dataStreamBuilder =
          new SourceDataStreamBuilder<>(executionEnvironment);
      DataStream<SojEvent> sourceDataStream = dataStreamBuilder
          .dc(DataCenter.of(dc))
          .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
          .uid(getString(Property.SOURCE_UID))
          .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
          .buildRescaled(new AvroKafkaDeserializationSchema<>(SojEvent.class));

      // hdfs sink
      sourceDataStream
          .addSink(HdfsConnectorFactory.createWithParquet(hdfsPath, SojEvent.class,
              new SojCommonDateTimeBucketAssigner<>()))
          .setParallelism(sinkParallelNum)
          .name(getString(Property.SINK_OPERATOR_NAME))
          .uid(getString(Property.SINK_UID));

    // submit job
    FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
  }
}
