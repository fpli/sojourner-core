package com.ebay.sojourner.ubd.rt.pipeline;

import com.ebay.sojourner.ubd.common.model.SojEvent;
import com.ebay.sojourner.ubd.rt.common.state.StateBackendFactory;
import com.ebay.sojourner.ubd.rt.connectors.filesystem.HdfsSinkUtil;
import com.ebay.sojourner.ubd.rt.connectors.kafka.KafkaSourceFunctionForLoad;
import com.ebay.sojourner.ubd.rt.util.AppEnv;
import com.ebay.sojourner.ubd.rt.util.Constants;
import com.ebay.sojourner.ubd.rt.util.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SojournerKafkaToHdfsJob {

  public static void main(String[] args) throws Exception {
    // Make sure this is being executed at start up.
    ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
    AppEnv.config(parameterTool);

    // 0.0 Prepare execution environment
    // 0.1 UBI configuration
    // 0.2 Flink configuration
    final StreamExecutionEnvironment executionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // checkpoint settings
    executionEnvironment.enableCheckpointing(
        AppEnv.config().getFlink().getCheckpoint().getInterval().getSeconds() * 1000,
        CheckpointingMode.EXACTLY_ONCE);
    executionEnvironment
        .getCheckpointConfig()
        .setCheckpointTimeout(
            AppEnv.config().getFlink().getCheckpoint().getTimeout().getSeconds() * 1000);
    executionEnvironment
        .getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(
            AppEnv.config().getFlink().getCheckpoint().getMinPauseBetween().getSeconds() * 1000);
    executionEnvironment
        .getCheckpointConfig()
        .setMaxConcurrentCheckpoints(
            AppEnv.config().getFlink().getCheckpoint().getMaxConcurrent() == null
                ? 1
                : AppEnv.config().getFlink().getCheckpoint().getMaxConcurrent());
    executionEnvironment.setStateBackend(
        StateBackendFactory.getStateBackend(StateBackendFactory.ROCKSDB));

    // kafka source for session
    /*
    DataStream<SojSession> sojSessionDataStream =
        executionEnvironment
            .addSource(KafkaSourceFunctionForLoad
                .generateWatermark(Constants.TOPIC_PRODUCER_SESSION,
                    Constants.BOOTSTRAP_SERVERS_SESSION, Constants.GROUP_ID_SESSION,
                    SojSession.class))
            .setParallelism(50)
            .name("Rheos Kafka Consumer For Session")
            .uid("kafkaSourceForSession");
            */

    // kafka source for event
    DataStream<SojEvent> sojEventDataStream =
        executionEnvironment
            .addSource(KafkaSourceFunctionForLoad
                .generateWatermark(Constants.TOPIC_PRODUCER_EVENT,
                    Constants.BOOTSTRAP_SERVERS_EVENT, Constants.GROUP_ID_EVENT,
                    SojEvent.class))
            .setParallelism(150)
            .name("Rheos Kafka Consumer For Event")
            .uid("kafkaSourceForEvent");

    // hdfs sink for session
    /*
    sojSessionDataStream
        .addSink(HdfsSinkUtil.sojSessionSinkWithParquet())
        .setParallelism(50)
        .name("SojSession sink")
        .uid("sessionHdfsSink")
        .disableChaining();
        */
    // hdfs sink for event
    sojEventDataStream
        .addSink(HdfsSinkUtil.sojEventSinkWithParquet())
        .setParallelism(50)
        .name("SojEvent sink")
        .uid("eventHdfsSink")
        .disableChaining();

    // Submit this job
    executionEnvironment.execute(AppEnv.config().getFlink().getApp().getNameForRTLoadPipeline());
  }
}
