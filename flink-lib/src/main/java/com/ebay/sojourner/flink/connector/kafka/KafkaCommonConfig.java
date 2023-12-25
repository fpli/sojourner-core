package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import java.util.Properties;
import org.apache.kafka.common.config.SaslConfigs;

@Deprecated
public class KafkaCommonConfig {

  private KafkaCommonConfig() {
    // empty
  }

  public static Properties get() {

    Properties props = new Properties();
    props.put(SaslConfigs.SASL_MECHANISM, "IAF");
    props.put("security.protocol", "SASL_PLAINTEXT");

    final String saslJaasConfig =
        String.format(
            "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required " +
                "iafConsumerId=\"%s\" iafSecret=\"%s\" iafEnv=\"%s\";",
            FlinkEnvUtils.getString(Property.RHEOS_CLIENT_ID),
            FlinkEnvUtils.getString(Property.RHEOS_CLIENT_IAF_SECRET),
            FlinkEnvUtils.getString(Property.RHEOS_CLIENT_IAF_ENV));

    props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
    return props;
  }
}
