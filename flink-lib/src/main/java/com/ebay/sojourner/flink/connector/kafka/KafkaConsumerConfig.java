package com.ebay.sojourner.flink.connector.kafka;

import static com.ebay.sojourner.common.util.Property.KAFKA_CONSUMER_BOOTSTRAP_SERVERS;
import static com.ebay.sojourner.common.util.Property.KAFKA_CONSUMER_GROUP_ID;
import static com.ebay.sojourner.common.util.Property.KAFKA_CONSUMER_TOPIC;
import static com.ebay.sojourner.flink.common.DataCenter.LVS;
import static com.ebay.sojourner.flink.common.DataCenter.RNO;
import static com.ebay.sojourner.flink.common.DataCenter.SLC;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getBoolean;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getListString;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Properties;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;

@Data
public class KafkaConsumerConfig {

  private KafkaConsumerConfig() {
    // empty
  }

  private DataCenter dc;
  private List<String> topics;
  private String brokers;
  private String groupId;
  private Properties properties;

  public static KafkaConsumerConfig ofDC(DataCenter dataCenter) {
    KafkaConsumerConfig config = new KafkaConsumerConfig();

    config.setDc(dataCenter);

    final List<String> topics = FlinkEnvUtils.getList(KAFKA_CONSUMER_TOPIC);
    final String groupId = getString(KAFKA_CONSUMER_GROUP_ID);

    Preconditions.checkState(CollectionUtils.isNotEmpty(topics));
    Preconditions.checkState(StringUtils.isNotBlank(groupId));

    config.setTopics(topics);
    config.setGroupId(groupId);

    switch (dataCenter) {
      case LVS:
        config.setBrokers(getBrokersForDC(LVS));
        break;
      case RNO:
        config.setBrokers(getBrokersForDC(RNO));
        break;
      case SLC:
        config.setBrokers(getBrokersForDC(SLC));
        break;
      default:
        throw new IllegalStateException("Cannot find datacenter kafka bootstrap servers");
    }

    config.setProperties(buildKafkaConsumerConfig(config.getBrokers(), config.getGroupId()));

    return config;
  }

  public static KafkaConsumerConfig ofDC(String dataCenter) {
    DataCenter dc = DataCenter.of(dataCenter);
    return ofDC(dc);
  }

  private static String getBrokersForDC(DataCenter dc) {
    String propKey = KAFKA_CONSUMER_BOOTSTRAP_SERVERS + "." + dc.getValue().toLowerCase();
    return getListString(propKey);
  }

  private static Properties buildKafkaConsumerConfig(String brokers, String groupId) {

    boolean kafkaConsumerAuthEnabled = true;
    try {
      // override default value if set
      kafkaConsumerAuthEnabled = getBoolean(Property.KAFKA_CONSUMER_AUTH_ENABLED);
    } catch (Exception e) {
      // use default value if not set
    }
    Properties consumerConfig = new Properties();
    if (kafkaConsumerAuthEnabled) {
      consumerConfig.putAll(KafkaCommonConfig.get());
    }

    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                       getInteger(Property.MAX_POLL_RECORDS));
    consumerConfig.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG,
                       getInteger(Property.RECEIVE_BUFFER));
    consumerConfig.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                       getInteger(Property.FETCH_MAX_BYTES));
    consumerConfig.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                       getInteger(Property.FETCH_MAX_WAIT_MS));
    consumerConfig.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                       getInteger(Property.MAX_PARTITIONS_FETCH_BYTES));
    consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                       RoundRobinAssignor.class.getName());
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                       getString(Property.AUTO_RESET_OFFSET));
    consumerConfig.put("flink.partition-discovery.interval-millis",
        getInteger(Property.PARTITION_DISCOVERY_INTERVAL_MS));
    return consumerConfig;
  }
}
