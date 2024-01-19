package com.ebay.sojourner.flink.common;

import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_CHECKPOINT_DATA_DIR;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_CHECKPOINT_INCREMENTAL;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_CHECKPOINT_INTERVAL_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_CHECKPOINT_MAX_CONCURRENT;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_CHECKPOINT_MIN_PAUSE_BETWEEN_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_CHECKPOINT_TIMEOUT_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_CHECKPOINT_TOLERATE_FAILURE_COUNT;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_NAME;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SINK;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_PARALLELISM_SOURCE;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_KAFKA_DC;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_KAFKA_ENV;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SINK_KAFKA_STREAM;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SOURCE_KAFKA_DC;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SOURCE_KAFKA_ENV;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SOURCE_KAFKA_FROM_TIMESTAMP;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SOURCE_KAFKA_GROUP_ID;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SOURCE_KAFKA_STREAM;
import static com.ebay.sojourner.common.constant.ConfigProperty.FLINK_APP_SOURCE_KAFKA_TOPIC;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_CONSUMER_AUTO_OFFSET_RESET;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_CONSUMER_FETCH_MAX_BYTES;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_CONSUMER_FETCH_MAX_WAIT_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_CONSUMER_MAX_PARTITIONS_FETCH_BYTES;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_CONSUMER_MAX_POLL_RECORDS;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_CONSUMER_PARTITION_DISCOVERY_INTERVAL_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_CONSUMER_RECEIVE_BUFFER;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_ACKS;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_BATCH_SIZE;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_BUFFER_MEMORY;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_COMPRESSION_TYPE;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_LINGER_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_MAX_REQUEST_SIZE;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_REQUEST_RETRIES;
import static com.ebay.sojourner.common.constant.ConfigProperty.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_CLIENT_AUTH_TYPE;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_CLIENT_IAF_ENV;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_CLIENT_IAF_ID;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_CLIENT_IAF_SECRET;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_CLIENT_TF_APP_ID;
import static com.ebay.sojourner.common.constant.ConfigProperty.RHEOS_CLIENT_TF_ENV;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CLASS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

import com.ebay.sojourner.common.env.EnvironmentUtils;
import com.ebay.sojourner.flink.connector.kafka.rheos.RheosStreamsConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.ebay.rheos.kafka.security.RheosLogin;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;


@Slf4j
public class FlinkEnv {

    private static final String PROFILE = "profile";

    private final Map<String, String> CONFIG = Maps.newHashMap();

    private boolean initialized = false;

    public FlinkEnv(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String profile = parameterTool.get(PROFILE);

        if (StringUtils.isNotBlank(profile)) {
            EnvironmentUtils.activateProfile(profile);
            CONFIG.put(PROFILE, profile);
        }

        EnvironmentUtils.sourceArgProps(parameterTool.getProperties());

        // load git.properties file if exists
        try (InputStream input = this.getClass().getClassLoader().getResourceAsStream("git.properties")) {
            if (input == null) {
                log.warn("Cannot find git.properties file in classpath");
                return;
            }
            // load git.properties and put props into CONFIG map
            Properties props = new Properties();
            props.load(input);
            for (String key : props.stringPropertyNames()) {
                CONFIG.put(key, props.getProperty(key));
            }
        } catch (IOException e) {
            log.warn("Error when loading git.properties file", e);
        }
    }

    public StreamExecutionEnvironment init(StreamExecutionEnvironment env) {

        if (env == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        // set runtime mode to Streaming
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // disable auto-generated uid by default, every job has to explicitly provide uid
        env.getConfig().disableAutoGeneratedUIDs();

        // checkpoint configs
        // default checkpoint interval set to 5m
        env.enableCheckpointing(getIntegerOrDefault(FLINK_APP_CHECKPOINT_INTERVAL_MS, 300000));

        // set default checkpoint configs
        CheckpointConfig conf = env.getCheckpointConfig();
        // default checkpoint mode set to exactly once
        conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        String checkpointStorage = getStringOrNull(FLINK_APP_CHECKPOINT_DATA_DIR);
        if (checkpointStorage != null) {
            if (!(checkpointStorage.startsWith("file://") || checkpointStorage.startsWith("hdfs://"))) {
                // for local mode
                checkpointStorage = "file://" + checkpointStorage;
            }
            conf.setCheckpointStorage(checkpointStorage);
        }

        // default min pause time set to 1m
        conf.setMinPauseBetweenCheckpoints(getIntegerOrDefault(FLINK_APP_CHECKPOINT_MIN_PAUSE_BETWEEN_MS, 60000));
        // default checkpoint timeout set to 5m
        conf.setCheckpointTimeout(getIntegerOrDefault(FLINK_APP_CHECKPOINT_TIMEOUT_MS, 300000));
        // default max concurrent checkpoint count set to 1
        conf.setMaxConcurrentCheckpoints(getIntegerOrDefault(FLINK_APP_CHECKPOINT_MAX_CONCURRENT, 1));
        // default tolerable checkpoint failure number set to Integer.MAX_VALUE
        conf.setTolerableCheckpointFailureNumber(
                getIntegerOrDefault(FLINK_APP_CHECKPOINT_TOLERATE_FAILURE_COUNT, Integer.MAX_VALUE));

        if (env.getStateBackend() == null) {
            Boolean incrementalCheckpoint = getBooleanOrDefault(FLINK_APP_CHECKPOINT_INCREMENTAL, false);
            EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(incrementalCheckpoint);
            rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
            env.setStateBackend(rocksDBStateBackend);
        }

        // set initialized to true
        initialized = true;
        return env;
    }

    public StreamExecutionEnvironment init() {
        return this.init(null);
    }

    public StreamExecutionEnvironment local() {
        // default TM count set to cpu cores
        int cores = Runtime.getRuntime().availableProcessors();
        return this.local(cores);
    }

    public StreamExecutionEnvironment local(int slots) {
        // default flink web ui port set to 9999
        return this.local(slots, 9999);
    }

    public StreamExecutionEnvironment local(int slots, int webUiPort) {
        final Configuration configuration = new Configuration();
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slots);
        configuration.setInteger(RestOptions.PORT, webUiPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setStateBackend(new HashMapStateBackend());

        return this.init(env);
    }

    public void execute(StreamExecutionEnvironment env, String jobName) throws Exception {
        Preconditions.checkNotNull(env);
        Preconditions.checkNotNull(jobName);
        Preconditions.checkState(this.initialized, "Please call init method first before executing");

        ParameterTool parameterTool = ParameterTool.fromMap(CONFIG);
        // make parameters available in the flink web ui
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.execute(jobName);
    }

    public void execute(StreamExecutionEnvironment env) throws Exception {
        Preconditions.checkNotNull(env);
        String jobName = this.getStringOrDefault(FLINK_APP_NAME, "My Flink Job");
        this.execute(env, jobName);
    }

    // env
    public String getString(String key) {
        String value = EnvironmentUtils.get(key);
        CONFIG.put(key, value);
        return value;
    }

    public String getStringOrNull(String key) {
        String value = EnvironmentUtils.getOrNull(key);
        if (value != null) {
            CONFIG.put(key, value);
        }
        return value;
    }

    public String getStringOrDefault(String key, String defaultValue) {
        String value = EnvironmentUtils.getStringOrDefault(key, defaultValue);
        CONFIG.put(key, value);
        return value;
    }

    public Integer getInteger(String key) {
        Integer value = EnvironmentUtils.getInteger(key);
        CONFIG.put(key, String.valueOf(value));
        return value;
    }

    public Integer getIntegerOrDefault(String key, Integer defaultValue) {
        Integer value = EnvironmentUtils.getIntegerOrDefault(key, defaultValue);
        CONFIG.put(key, String.valueOf(value));
        return value;
    }

    public Long getLong(String key) {
        Long value = EnvironmentUtils.getLong(key);
        CONFIG.put(key, String.valueOf(value));
        return value;
    }

    public Boolean getBoolean(String key) {
        Boolean value = EnvironmentUtils.getBoolean(key);
        CONFIG.put(key, String.valueOf(value));
        return value;
    }

    public Boolean getBooleanOrDefault(String key, boolean defaultValue) {
        Boolean value = EnvironmentUtils.getBooleanOrDefault(key, defaultValue);
        CONFIG.put(key, String.valueOf(value));
        return value;
    }

    public String getListAsString(String key) {
        List<String> list = EnvironmentUtils.getForClass(key, List.class);
        String value = String.join(",", list);
        CONFIG.put(key, value);
        return value;
    }

    public Set<String> getSet(String key) {
        List<String> list = EnvironmentUtils.getForClass(key, List.class);
        String value = String.join(",", list);
        CONFIG.put(key, value);
        return new HashSet<>(list);
    }

    public List<String> getList(String key) {
        List<String> list = EnvironmentUtils.getForClass(key, List.class);
        String value = String.join(",", list);
        CONFIG.put(key, value);
        return list;
    }

    public String[] getStringArray(String key, String delimiter) {
        String value = EnvironmentUtils.get(key);
        CONFIG.put(key, value);
        return EnvironmentUtils.getStringArray(key, delimiter);
    }

    public List<String> getStringList(String key, String delimiter) {
        String value = EnvironmentUtils.get(key);
        CONFIG.put(key, value);
        return EnvironmentUtils.getStringList(key, delimiter);
    }

    // env shortcuts
    public Integer getSourceParallelism() {
        return this.getInteger(FLINK_APP_PARALLELISM_SOURCE);
    }

    public Integer getSinkParallelism() {
        return this.getInteger(FLINK_APP_PARALLELISM_SINK);
    }

    // kafka client configs
    public Properties getKafkaConsumerProps() {
        String env = this.getString(FLINK_APP_SOURCE_KAFKA_ENV);
        String stream = this.getString(FLINK_APP_SOURCE_KAFKA_STREAM);

        return this.getKafkaConsumerProps(env, stream);
    }

    public Properties getKafkaConsumerProps(String env, String stream) {
        Preconditions.checkNotNull(env);
        Preconditions.checkNotNull(stream);

        SecurityProtocol protocol = RheosStreamsConfig.getAuthProtocol(env, stream);
        CONFIG.put("rheos.kafka." + stream + ".security-protocol", protocol.toString());

        Properties props = new Properties();

        // only support SASL_PLAINTEXT streams, others are not supported at this moment
        if (protocol.equals(SecurityProtocol.SASL_PLAINTEXT)) {
            String authType = this.getString(RHEOS_CLIENT_AUTH_TYPE);
            props.put(SASL_LOGIN_CLASS, RheosLogin.class);
            props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
            props.put(SASL_MECHANISM, authType);
            props.put(SASL_JAAS_CONFIG, getSaslJaasConfig(authType));
        } else {
            throw new RuntimeException("SecurityProtocol is not supported");
        }

        // partition.discovery.interval.ms, type: long
        if (EnvironmentUtils.isSet(KAFKA_CONSUMER_PARTITION_DISCOVERY_INTERVAL_MS)) {
            Long intervalMs = this.getLong(KAFKA_CONSUMER_PARTITION_DISCOVERY_INTERVAL_MS);
            props.put(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), intervalMs);
        } else {
            // default enable partition auto discovery and interval set to 1m
            props.put(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), 60000);
        }

        // max.poll.records, type: int
        if (EnvironmentUtils.isSet(KAFKA_CONSUMER_MAX_POLL_RECORDS)) {
            Integer maxPollRecords = this.getInteger(KAFKA_CONSUMER_MAX_POLL_RECORDS);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        }

        // fetch.max.bytes, type: int
        if (EnvironmentUtils.isSet(KAFKA_CONSUMER_FETCH_MAX_BYTES)) {
            Integer fetchMaxBytes = this.getInteger(KAFKA_CONSUMER_FETCH_MAX_BYTES);
            props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        }

        // receive.buffer.bytes, type: int
        if (EnvironmentUtils.isSet(KAFKA_CONSUMER_RECEIVE_BUFFER)) {
            Integer receiveBuffer = this.getInteger(KAFKA_CONSUMER_RECEIVE_BUFFER);
            props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBuffer);
        }

        // fetch.max.wait.ms, type: int
        if (EnvironmentUtils.isSet(KAFKA_CONSUMER_FETCH_MAX_WAIT_MS)) {
            Integer fetchMaxWaitMs = this.getInteger(KAFKA_CONSUMER_FETCH_MAX_WAIT_MS);
            props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        }

        // max.partition.fetch.bytes, type: int
        if (EnvironmentUtils.isSet(KAFKA_CONSUMER_MAX_PARTITIONS_FETCH_BYTES)) {
            Integer maxPartitionFetchBytes = this.getInteger(KAFKA_CONSUMER_MAX_PARTITIONS_FETCH_BYTES);
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        }

        // auto.offset.reset, type: string
        if (EnvironmentUtils.isSet(KAFKA_CONSUMER_AUTO_OFFSET_RESET)) {
            String autoOffsetReset = this.getString(KAFKA_CONSUMER_AUTO_OFFSET_RESET);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        }

        return props;
    }

    public Properties getKafkaProducerProps() {
        String env = this.getString(FLINK_APP_SINK_KAFKA_ENV);
        String stream = this.getString(FLINK_APP_SINK_KAFKA_STREAM);

        return this.getKafkaProducerProps(env, stream);
    }

    public Properties getKafkaProducerProps(String env, String stream) {
        Preconditions.checkNotNull(env);
        Preconditions.checkNotNull(stream);

        Properties props = new Properties();

        SecurityProtocol protocol = RheosStreamsConfig.getAuthProtocol(env, stream);
        CONFIG.put("rheos.kafka." + stream + ".security-protocol", protocol.toString());

        // only support SASL_PLAINTEXT streams, others are not supported at this moment
        if (protocol.equals(SecurityProtocol.SASL_PLAINTEXT)) {
            String authType = this.getString(RHEOS_CLIENT_AUTH_TYPE);
            props.put(SASL_LOGIN_CLASS, RheosLogin.class);
            props.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
            props.put(SASL_MECHANISM, authType);
            props.put(SASL_JAAS_CONFIG, getSaslJaasConfig(authType));
        } else {
            throw new RuntimeException("SecurityProtocol is not supported");
        }

        // acks, type: string, set default to 1
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_ACKS)) {
            String ack = this.getString(KAFKA_PRODUCER_ACKS);
            props.put(ProducerConfig.ACKS_CONFIG, ack);
        } else {
            props.put(ProducerConfig.ACKS_CONFIG, "1");
        }

        // batch.size, type: int
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_BATCH_SIZE)) {
            Integer batchSize = this.getInteger(KAFKA_PRODUCER_BATCH_SIZE);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        }

        // request.timeout.ms, type: int
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
            Integer requestTimeoutMs = this.getInteger(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        }

        // delivery.timeout.ms, type: int
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS)) {
            Integer deliveryTimeoutMs = this.getInteger(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS);
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        }

        // retries, type: int
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_REQUEST_RETRIES)) {
            Integer requestRetries = this.getInteger(KAFKA_PRODUCER_REQUEST_RETRIES);
            props.put(ProducerConfig.RETRIES_CONFIG, requestRetries);
        }

        // linger.ms, type: long
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_LINGER_MS)) {
            Long lingerMs = this.getLong(KAFKA_PRODUCER_LINGER_MS);
            props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        }

        // buffer.memory, type: long
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_BUFFER_MEMORY)) {
            Long bufferMemory = this.getLong(KAFKA_PRODUCER_BUFFER_MEMORY);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        }

        // compression.type, type: string
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_COMPRESSION_TYPE)) {
            String compressionType = this.getString(KAFKA_PRODUCER_COMPRESSION_TYPE);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }

        // max.request.size, type: int
        if (EnvironmentUtils.isSet(KAFKA_PRODUCER_MAX_REQUEST_SIZE)) {
            Integer maxRequestSize = this.getInteger(KAFKA_PRODUCER_MAX_REQUEST_SIZE);
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        }

        return props;

    }

    public OffsetsInitializer getSourceKafkaStartingOffsets() {
        if (EnvironmentUtils.isSet(FLINK_APP_SOURCE_KAFKA_FROM_TIMESTAMP)) {
            String fromTimestamp = getString(FLINK_APP_SOURCE_KAFKA_FROM_TIMESTAMP);

            if (fromTimestamp.equalsIgnoreCase("0")
                    || fromTimestamp.equalsIgnoreCase("latest")) {
                return OffsetsInitializer.latest();
            } else if (fromTimestamp.equalsIgnoreCase("earliest")) {
                return OffsetsInitializer.earliest();
            } else {
                return OffsetsInitializer.timestamp(Long.parseLong(fromTimestamp));
            }
        }

        return OffsetsInitializer.latest();
    }

    public String getSourceKafkaGroupId() {
        return getString(FLINK_APP_SOURCE_KAFKA_GROUP_ID);
    }

    public List<String> getSourceKafkaTopics() {
        return getList(FLINK_APP_SOURCE_KAFKA_TOPIC);
    }

    private String getSaslJaasConfig(String authType) {
        switch (authType.toUpperCase()) {
            case "IAF":
                return String.format("io.ebay.rheos.kafka.security.iaf.IAFLoginModule required "
                                             + "iafConsumerId=\"%s\" iafSecret=\"%s\" iafEnv=\"%s\";",
                                     this.getString(RHEOS_CLIENT_IAF_ID),
                                     this.getString(RHEOS_CLIENT_IAF_SECRET),
                                     this.getString(RHEOS_CLIENT_IAF_ENV));

            // Rheos Doc: https://pages.github.corp.ebay.com/Near-Real-Time/rheos-documentation/docs/streaming/TF-Sample-Code/#flink-job-client-sample
            case "TRUST_FABRIC":
                String clientEnv = getStringOrNull(RHEOS_CLIENT_TF_ENV);
                if (clientEnv != null && clientEnv.equalsIgnoreCase("local")) {
                    return String.format("io.ebay.rheos.kafka.security.tf.TFDevClientLoginModule required "
                                                 + "appInstance=\"%s\" appName=\"%s\" env=local;",
                                         this.getString(RHEOS_CLIENT_TF_APP_ID),
                                         this.getString(RHEOS_CLIENT_TF_APP_ID));
                } else {
                    // in rheos env, no need to config appInstance etc.
                    return "io.ebay.rheos.kafka.security.tf.TFClientLoginModule required ;";
                }
            default:
                throw new RuntimeException("Rheos client auth type " + authType + " is not supported");
        }
    }

    // kafka env
    public String getSourceKafkaStreamName() {
        return this.getString(FLINK_APP_SOURCE_KAFKA_STREAM);
    }

    public String getSourceKafkaBrokers() {
        String env = this.getString(FLINK_APP_SOURCE_KAFKA_ENV);
        String stream = this.getString(FLINK_APP_SOURCE_KAFKA_STREAM);
        String dc = this.getString(FLINK_APP_SOURCE_KAFKA_DC);

        return getKafkaBrokers(env, stream, dc);
    }

    public String getSourceKafkaBrokersOfDC(DataCenter dc) {
        String env = this.getString(FLINK_APP_SOURCE_KAFKA_ENV);
        String stream = this.getString(FLINK_APP_SOURCE_KAFKA_STREAM);

        return getKafkaBrokers(env, stream, dc.toString());
    }

    public String getSinkKafkaStreamName() {
        return this.getString(FLINK_APP_SINK_KAFKA_STREAM);
    }

    public String getSinkKafkaBrokers() {
        String env = this.getString(FLINK_APP_SINK_KAFKA_ENV);
        String stream = this.getString(FLINK_APP_SINK_KAFKA_STREAM);
        String dc = this.getString(FLINK_APP_SINK_KAFKA_DC);

        return getKafkaBrokers(env, stream, dc);
    }

    public String getKafkaBrokers(String env, String stream, String dc) {
        Preconditions.checkNotNull(env);
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(dc);

        String brokers = RheosStreamsConfig.getBrokersAsString(env, stream, DataCenter.of(dc));
        CONFIG.put("rheos.kafka." + stream + "." + dc + ".brokers", brokers);

        return brokers;
    }

}
