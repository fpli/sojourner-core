package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.DataCenter;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import static com.ebay.sojourner.common.util.Property.KAFKA_PRODUCER_BOOTSTRAP_SERVERS;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getBoolean;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getInteger;
import static com.ebay.sojourner.flink.common.FlinkEnvUtils.getString;

@Deprecated
@Data
public class KafkaProducerConfig {

  private KafkaProducerConfig(DataCenter dc) {
    this.dc = dc;
  }

  private final DataCenter dc;
  private String brokers;
  private Properties properties;

  public static KafkaProducerConfig ofDC(DataCenter dataCenter) {
    Preconditions.checkNotNull(dataCenter);

    KafkaProducerConfig config = new KafkaProducerConfig(dataCenter);

    switch (dataCenter) {
      case LVS:
        config.setBrokers(getBrokersForDC(DataCenter.LVS));
        break;
      case RNO:
        config.setBrokers(getBrokersForDC(DataCenter.RNO));
        break;
      case SLC:
        config.setBrokers(getBrokersForDC(DataCenter.SLC));
        break;
      default:
        throw new IllegalStateException("Cannot find datacenter kafka bootstrap servers");
    }

    config.setProperties(buildKafkaProducerConfig(config.getBrokers()));

    return config;
  }

  public static KafkaProducerConfig ofDC(String dataCenter) {
    DataCenter dc = DataCenter.of(dataCenter);
    return ofDC(dc);
  }

  private static String getBrokersForDC(DataCenter dc) {
    String propKey = KAFKA_PRODUCER_BOOTSTRAP_SERVERS + "." + dc.getValue().toLowerCase();
    return FlinkEnvUtils.getListString(propKey);
  }

  private static Properties buildKafkaProducerConfig(String brokers) {
    boolean kafkaProducerAuthEnabled = true;
    try {
      // override default value if set
      kafkaProducerAuthEnabled = getBoolean(Property.KAFKA_PRODUCER_AUTH_ENABLED);
    } catch (Exception e) {
      // use default value if not set
    }
    Properties producerConfig = new Properties();
    if (kafkaProducerAuthEnabled) {
      producerConfig.putAll(KafkaCommonConfig.get());
    }

    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG,
                       getInteger(Property.BATCH_SIZE));
    producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                       getInteger(Property.REQUEST_TIMEOUT_MS));
    producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                       getInteger(Property.DELIVERY_TIMEOUT_MS));
    producerConfig.put(ProducerConfig.RETRIES_CONFIG,
                       getInteger(Property.REQUEST_RETRIES));
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG,
                       getInteger(Property.LINGER_MS));
    producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                       getInteger(Property.BUFFER_MEMORY));
    producerConfig.put(ProducerConfig.ACKS_CONFIG,
                       getString(Property.ACKS));
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                       getString(Property.COMPRESSION_TYPE));
    producerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                       getInteger(Property.MAX_REQUEST_SIZE));
    // need to confirm if one flink job only runs with one type of consumer/producer (Common or HA)
    producerConfig.put("sasl.login.class", "io.ebay.rheos.kafka.security.RheosLogin");

    return producerConfig;
  }
}
