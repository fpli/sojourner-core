package com.ebay.sojourner.common.constant;

public class ConfigProperty {

  // ----------------------- flink property -----------------------
  // flink - app
  public static final String FLINK_APP_NAME = "flink.app.name";
  public static final String FLINK_APP_DEBUG_MODE = "flink.app.debug-mode";

  // flink - parallelism
  public static final String FLINK_APP_PARALLELISM_DEFAULT = "flink.app.parallelism.default";
  public static final String FLINK_APP_PARALLELISM_SOURCE = "flink.app.parallelism.source";
  public static final String FLINK_APP_PARALLELISM_PROCESS = "flink.app.parallelism.process";
  public static final String FLINK_APP_PARALLELISM_SINK = "flink.app.parallelism.sink";

  // flink - checkpoint
  public static final String FLINK_APP_CHECKPOINT_MODE = "flink.app.checkpoint.mode";
  public static final String FLINK_APP_CHECKPOINT_INCREMENTAL = "flink.app.checkpoint.incremental";
  public static final String FLINK_APP_CHECKPOINT_DATA_DIR = "flink.app.checkpoint.data-dir";
  public static final String FLINK_APP_CHECKPOINT_INTERVAL_MS = "flink.app.checkpoint.interval-ms";
  public static final String FLINK_APP_CHECKPOINT_TIMEOUT_MS = "flink.app.checkpoint.timeout-ms";
  public static final String FLINK_APP_CHECKPOINT_MIN_PAUSE_BETWEEN_MS = "flink.app.checkpoint.min-pause-between-ms";
  public static final String FLINK_APP_CHECKPOINT_MAX_CONCURRENT = "flink.app.checkpoint.max-concurrent";
  public static final String FLINK_APP_CHECKPOINT_TOLERATE_FAILURE_COUNT = "flink.app.checkpoint.tolerate-failure-count";

  // flink - watermark & window
  public static final String FLINK_APP_WATERMARK_MAX_OUT_OF_ORDERNESS_IN_MIN = "flink.app.watermark.max-out-of-orderness-in-min";
  public static final String FLINK_APP_WATERMARK_IDLE_SOURCE_TIMEOUT_IN_MIN = "flink.app.watermark.idle-source-timeout-in-min";

  // flink - source - kafka
  public static final String FLINK_APP_SOURCE_KAFKA_ENV = "flink.app.source.kafka.env";
  public static final String FLINK_APP_SOURCE_KAFKA_STREAM = "flink.app.source.kafka.stream";
  public static final String FLINK_APP_SOURCE_KAFKA_DC = "flink.app.source.kafka.dc";
  public static final String FLINK_APP_SOURCE_KAFKA_TOPIC = "flink.app.source.kafka.topic";
  public static final String FLINK_APP_SOURCE_KAFKA_GROUP_ID = "flink.app.source.kafka.group-id";
  public static final String FLINK_APP_SOURCE_KAFKA_START_OFFSET = "flink.app.source.kafka.start-offset";

  // flink - sink - kafka
  public static final String FLINK_APP_SINK_KAFKA_ENV = "flink.app.sink.kafka.env";
  public static final String FLINK_APP_SINK_KAFKA_STREAM = "flink.app.sink.kafka.stream";
  public static final String FLINK_APP_SINK_KAFKA_DC = "flink.app.sink.kafka.dc";

  // flink - sink - hdfs
  public static final String FLINK_APP_SINK_HDFS_CLUSTER = "flink.app.sink.hdfs.cluster";
  public static final String FLINK_APP_SINK_HDFS_BASE_PATH = "flink.app.sink.hdfs.base-path";

  // ----------------------- kafka property -----------------------
  // kafka consumer
  public static final String KAFKA_CONSUMER_PARTITION_DISCOVERY_INTERVAL_MS = "kafka.consumer.partition-discovery-interval-ms";
  public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS = "kafka.consumer.max-poll-records";
  public static final String KAFKA_CONSUMER_RECEIVE_BUFFER = "kafka.consumer.receive-buffer";
  public static final String KAFKA_CONSUMER_FETCH_MAX_BYTES = "kafka.consumer.fetch-max-bytes";
  public static final String KAFKA_CONSUMER_FETCH_MAX_WAIT_MS = "kafka.consumer.fetch-max-wait-ms";
  public static final String KAFKA_CONSUMER_MAX_PARTITIONS_FETCH_BYTES = "kafka.consumer.max-partitions-fetch-bytes";
  public static final String KAFKA_CONSUMER_PARTITION_ASSIGNOR_CLASS = "kafka.consumer.partition-assignor-class";
  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "kafka.consumer.auto-offset-reset";

  // kafka producer
  public static final String KAFKA_PRODUCER_ACKS = "kafka.producer.acks";
  public static final String KAFKA_PRODUCER_BATCH_SIZE = "kafka.producer.batch-size";
  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS = "kafka.producer.request-timeout-ms";
  public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS = "kafka.producer.delivery-timeout-ms";
  public static final String KAFKA_PRODUCER_REQUEST_RETRIES = "kafka.producer.request-retries";
  public static final String KAFKA_PRODUCER_LINGER_MS = "kafka.producer.linger-ms";
  public static final String KAFKA_PRODUCER_BUFFER_MEMORY = "kafka.producer.buffer-memory";
  public static final String KAFKA_PRODUCER_COMPRESSION_TYPE = "kafka.producer.compression-type";
  public static final String KAFKA_PRODUCER_MAX_REQUEST_SIZE = "kafka.producer.max-request-size";

  // rheos
  public static final String RHEOS_REGISTRY_URL = "rheos.registry-url";
  public static final String RHEOS_CLIENT_AUTH_TYPE = "rheos.client.auth-type";
  public static final String RHEOS_CLIENT_IAF_ID = "rheos.client.iaf.id";
  public static final String RHEOS_CLIENT_IAF_SECRET = "rheos.client.iaf.secret";
  public static final String RHEOS_CLIENT_IAF_ENV = "rheos.client.iaf.env";
  public static final String RHEOS_CLIENT_TF_APP_ID = "rheos.client.tf.app-id";
  public static final String RHEOS_CLIENT_TF_ENV = "rheos.client.tf.env";

  // rest client
  public static final String REST_CLIENT_BASE_URL = "rest-client.base-url";
  public static final String REST_CLIENT_CONFIG_PROFILE = "rest-client.config.profile";
  public static final String REST_CLIENT_CONFIG_PULL_INTERVAL = "rest-client.config.pull-interval";

  // region CJS properties
  public static final String CJS_PARSER_DISABLED = "cjs.parser.disabled";
  public static final String CJS_PARSER_FILTER_ENABLED = "cjs.parser.filter.enabled";
  public static final String CJS_CJS_DISABLED = "cjs.cjs.disabled";
  public static final String CJS_CJSBETA_DISABLED = "cjs.cjsBeta.disabled";
  //endregion
}
