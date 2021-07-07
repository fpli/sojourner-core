package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.common.util.Constants;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;

@Slf4j
public class SojFlinkKafkaProducer<T> extends FlinkKafkaProducer<T> {

  private final boolean allowDrop;
  private Counter droppedMessageCounter;
  private Counter droppedBatchCounter;
  private static final String DROPPED_MESSAGE_METRIC_NAME = "producer-dropped-message-count";
  private static final String DROPPED_BATCH_METRIC_NAME = "producer-dropped-batch-count";

  public SojFlinkKafkaProducer(String defaultTopic, KafkaSerializationSchema<T> serializationSchema,
      Properties producerConfig, Semantic semantic, boolean allowDrop) {
    super(defaultTopic, serializationSchema, producerConfig, semantic);
    this.allowDrop = allowDrop;
  }

  @Override
  public void open(Configuration configuration) throws Exception {
    super.open(configuration);
    log.info("Flink kafka producer drop event switch is: " + allowDrop);
    if (allowDrop) {
      // dropped message or batch monitoring
      droppedMessageCounter =
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(Constants.SOJ_METRICS_GROUP)
              .counter(DROPPED_MESSAGE_METRIC_NAME);

      droppedBatchCounter =
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(Constants.SOJ_METRICS_GROUP)
              .counter(DROPPED_BATCH_METRIC_NAME);

      this.callback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (exception != null && asyncException == null) {
            if (exception instanceof RecordTooLargeException) {
              log.error("The record is dropped because it's too large.", exception);
              droppedMessageCounter.inc();
            } else if (exception instanceof RecordBatchTooLargeException) {
              log.error("The record is dropped because the batch is too large." + exception);
              droppedBatchCounter.inc();
            } else {
              asyncException = exception;
            }
          }
          acknowledgeMessage();
        }
      };
    }
  }
}
