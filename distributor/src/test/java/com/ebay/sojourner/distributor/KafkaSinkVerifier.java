package com.ebay.sojourner.distributor;

import com.ebay.sojourner.common.env.EnvironmentUtils;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.event.RheosEvent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.config.SaslConfigs;

public class KafkaSinkVerifier {
  public static void main(String[] args) {
    Properties props = new Properties();
    // This is the broker addresses that can be looked up in Rheos portal
    EnvironmentUtils.activateProfile("test");
    String brokers = String.join(",", EnvironmentUtils.getForClass("kafka.consumer.bootstrap-servers.lvs", List.class));
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    // This is the consumer name that is registered in Rheos
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sojourner-distributor-test-sink-verifier");
    // Rheos uses byte[] as key
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    // Rheos event deserializer that decodes messages stored in Kafka to RheosEvent. This is optional if you dont use RheosEvent
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.ebay.rheos.schema.avro.RheosEventDeserializer");

    props.put("sasl.mechanism", "IAF");
    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put(SaslConfigs.SASL_JAAS_CONFIG,
              "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:68a97ac2-013b-4915-9ed7-d6ae2ff01618\" iafSecret=\"6218c197-200e-49d7-b404-2a4dbf7595ef\" iafEnv=\"staging\";");

        /* below configs are up to users to define.
        Full config list can be found at http://kafka.apache.org/documentation.html#newconsumerconfigs
        */
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024);
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

    final KafkaConsumer<byte[], RheosEvent> kafkaConsumer = new KafkaConsumer<>(props);
    kafkaConsumer.subscribe(Arrays.asList("behavior.trafficjam.custimized.3962"));

    // Setup the schema config. This is optional if you dont use RheosEvent
    Map<String, Object> config = new HashMap<>();
    // for staging
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");
    // for production
    //config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.stratus.ebay.com");
    GenericRecordDomainDataDecoder decoder = new GenericRecordDomainDataDecoder(config);

    while (true) {
      ConsumerRecords<byte[], RheosEvent> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);
      Iterator<ConsumerRecord<byte[], RheosEvent>> iterator = consumerRecords.iterator();
      while (iterator.hasNext()) {
        ConsumerRecord<byte[], RheosEvent> record = iterator.next();
        System.out.println(decoder.decode(record.value()));
        System.out.println(new String(record.headers().lastHeader("schemaVersion").value()));
      }
    }
  }
}
