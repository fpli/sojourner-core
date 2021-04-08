package com.ebay.sojourner.dumper.pipeline;

import static com.ebay.sojourner.common.util.Property.FLINK_APP_SOURCE_FROM_TIMESTAMP;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.JetStreamOutputEvent;
import com.ebay.sojourner.common.model.JetStreamOutputSession;
import com.ebay.sojourner.common.model.PulsarEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.ebay.sojourner.flink.connector.hdfs.HdfsConnectorFactory;
import com.ebay.sojourner.flink.connector.hdfs.SojCommonDateTimeBucketAssigner;
import com.ebay.sojourner.flink.connector.kafka.SourceDataStreamBuilder;
import com.ebay.sojourner.flink.connector.kafka.schema.AvroKafkaDeserializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.JetstreamEventDeserializationSchema;
import com.ebay.sojourner.flink.connector.kafka.schema.JetstreamSessionDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

public class SojournerKafkaToHdfsJob {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment executionEnvironment = FlinkEnvUtils.prepare(args);

    Class<?> deserializeClass = Class.forName(getString(Property.FLINK_APP_SINK_HDFS_CLASS));
    String hdfsPath = getString(Property.FLINK_APP_SINK_HDFS_PATH);
    int sinkParallelNum = FlinkEnvUtils.getInteger(Property.SINK_HDFS_PARALLELISM);
    String dc = getString(Property.FLINK_APP_SOURCE_DC);

    if (deserializeClass.isAssignableFrom(JetStreamOutputEvent.class)) {
      SourceDataStreamBuilder<JetStreamOutputEvent> dataStreamBuilder =
          new SourceDataStreamBuilder<>(executionEnvironment);
      DataStream<JetStreamOutputEvent> sourceDataStream = dataStreamBuilder
          .dc(DataCenter.of(dc))
          .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
          .uid(getString(Property.SOURCE_UID))
          .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
          .buildRescaled(
              new KafkaDeserializationSchemaWrapper<>(new JetstreamEventDeserializationSchema()));
      // hdfs sink
      sourceDataStream
          .addSink(HdfsConnectorFactory.createWithParquet(hdfsPath, JetStreamOutputEvent.class,
              new SojCommonDateTimeBucketAssigner()))
          .setParallelism(sinkParallelNum)
          .name(getString(Property.SINK_OPERATOR_NAME))
          .uid(getString(Property.SINK_UID));

    } else if (deserializeClass.isAssignableFrom(JetStreamOutputSession.class)) {

      SourceDataStreamBuilder<JetStreamOutputSession> dataStreamBuilder =
          new SourceDataStreamBuilder<>(executionEnvironment);
      DataStream<JetStreamOutputSession> sourceDataStream = dataStreamBuilder
          .dc(DataCenter.of(dc))
          .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
          .uid(getString(Property.SOURCE_UID))
          .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
          .buildRescaled(
              new KafkaDeserializationSchemaWrapper<>(new JetstreamSessionDeserializationSchema()));

      // hdfs sink
      sourceDataStream
          .addSink(HdfsConnectorFactory.createWithParquet(hdfsPath, JetStreamOutputSession.class,
              new SojCommonDateTimeBucketAssigner<>()))
          .setParallelism(sinkParallelNum)
          .name(getString(Property.SINK_OPERATOR_NAME))
          .uid(getString(Property.SINK_UID));
    } else if (deserializeClass.isAssignableFrom(BotSignature.class)) {

      SourceDataStreamBuilder<BotSignature> dataStreamBuilder =
          new SourceDataStreamBuilder<>(executionEnvironment);
      DataStream<BotSignature> sourceDataStream = dataStreamBuilder
          .dc(DataCenter.of(dc))
          .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
          .uid(getString(Property.SOURCE_UID))
          .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
          .buildRescaled(new AvroKafkaDeserializationSchema<>(BotSignature.class));

      // hdfs sink
      sourceDataStream
          .addSink(HdfsConnectorFactory.createWithParquet(hdfsPath, BotSignature.class,
              new SojCommonDateTimeBucketAssigner<>()))
          .setParallelism(sinkParallelNum)
          .name(getString(Property.SINK_OPERATOR_NAME))
          .uid(getString(Property.SINK_UID));
    } else if (deserializeClass.isAssignableFrom(PulsarEvent.class)) {
      SourceDataStreamBuilder<PulsarEvent> dataStreamBuilder =
          new SourceDataStreamBuilder<>(executionEnvironment);

      DataStream<PulsarEvent> sourceDataStream = dataStreamBuilder
          .dc(DataCenter.of(dc))
          .operatorName(getString(Property.SOURCE_OPERATOR_NAME))
          .uid(getString(Property.SOURCE_UID))
          .fromTimestamp(getString(FLINK_APP_SOURCE_FROM_TIMESTAMP))
          .buildRescaled(new AvroKafkaDeserializationSchema<>(PulsarEvent.class));

      // hdfs sink
      sourceDataStream
          .addSink(HdfsConnectorFactory.createWithParquet(hdfsPath, PulsarEvent.class,
              new SojCommonDateTimeBucketAssigner<>()))
          .setParallelism(sinkParallelNum)
          .name(getString(Property.SINK_OPERATOR_NAME))
          .uid(getString(Property.SINK_UID));
    }

    // submit job
    FlinkEnvUtils.execute(executionEnvironment, getString(Property.FLINK_APP_NAME));
  }
}
