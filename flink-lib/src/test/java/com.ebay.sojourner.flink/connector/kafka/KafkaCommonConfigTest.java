package com.ebay.sojourner.flink.connector.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;

class KafkaCommonConfigTest {

  @Test
  void get() {
    Properties properties = KafkaCommonConfig.get();
    String saslMechanism = properties.getProperty(SaslConfigs.SASL_MECHANISM);
    String protocol = properties.getProperty("security.protocol");
    String saslJaasConfig = properties.getProperty(SaslConfigs.SASL_JAAS_CONFIG);
    String expectJaasConfig = "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"test\" iafSecret=\"test\" iafEnv=\"test\";";
    assertThat(saslMechanism).isEqualTo("IAF");
    assertThat(protocol).isEqualTo("SASL_PLAINTEXT");
    assertThat(saslJaasConfig).isEqualTo(expectJaasConfig);
  }
}