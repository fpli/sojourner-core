package com.ebay.sojourner.integration.function;

import com.ebay.sojourner.common.model.SojEvent;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroDeserializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class BytesArrayToRheosEventSojEventMapFunction extends RichMapFunction<byte[], SojEvent> {

    private final String registryUrl;
    private transient SchemaRegistryAwareAvroDeserializerHelper<SojEvent> deserializerHelper;
    private transient GenericRecordDomainDataDecoder genericRecordDomainDataDecoder;
    private transient RheosEventDeserializer rheosEventDeserializer;

    public BytesArrayToRheosEventSojEventMapFunction(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, this.registryUrl);
        this.deserializerHelper = new SchemaRegistryAwareAvroDeserializerHelper<>(config, SojEvent.class);
        this.genericRecordDomainDataDecoder = new GenericRecordDomainDataDecoder(config);
        this.rheosEventDeserializer = new RheosEventDeserializer();
    }

    @Override
    public SojEvent map(byte[] value) throws Exception {

        // test if RheosEventDeserializer is working
        RheosEvent rheosEvent = rheosEventDeserializer.deserialize(null, value);

        // test if GenericRecordDomainDataDecoder is working
        GenericRecord genericRecord = genericRecordDomainDataDecoder.decode(rheosEvent);

        // test if SchemaRegistryAwareAvroDeserializerHelper is working
        int schemaId = rheosEvent.getSchemaId();
        SojEvent element = deserializerHelper.deserializeData(schemaId, rheosEvent.toBytes());

        log.debug("schemaId: {}, element: {}", rheosEvent.getSchemaId(), element);

        return element;
    }


}
