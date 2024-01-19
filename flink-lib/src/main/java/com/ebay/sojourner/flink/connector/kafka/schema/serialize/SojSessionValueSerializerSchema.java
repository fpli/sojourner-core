package com.ebay.sojourner.flink.connector.kafka.schema.serialize;

import com.ebay.sojourner.common.model.SojSession;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SojSessionValueSerializerSchema implements SerializationSchema<SojSession> {

    private final String schemaRegistryUrl;
    private final String subjectName;

    private transient DatumWriter<RheosEvent> writer;
    private transient SchemaRegistryAwareAvroSerializerHelper<SojSession> serializerHelper;
    private int schemaId;

    public SojSessionValueSerializerSchema(String schemaRegistryUrl, String subjectName) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.subjectName = subjectName;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        log.info("Initialize DatumWriter and SchemaRegistryAwareAvroSerializerHelper");
        this.writer = new GenericDatumWriter<>(SojSession.getClassSchema());

        Map<String, Object> map = new HashMap<>();
        map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, schemaRegistryUrl);
        this.serializerHelper = new SchemaRegistryAwareAvroSerializerHelper<>(map, SojSession.class);
        this.schemaId = serializerHelper.getSchemaId(subjectName);
    }

    @Override
    public byte[] serialize(SojSession element) {

        // convert SpecificRecord to GenericRecord
        GenericRecord record = GenericData.get().deepCopy(SojSession.getClassSchema(), element);
        // assemble RheosEvent
        RheosEvent rheosEvent = new RheosEvent(record);
        rheosEvent.setEventCreateTimestamp(System.currentTimeMillis());
        rheosEvent.setEventSentTimestamp(System.currentTimeMillis());
        rheosEvent.setSchemaId(schemaId);
        rheosEvent.setProducerId("sojourner");

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] serializedValue = null;
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(rheosEvent, encoder);
            encoder.flush();
            serializedValue = out.toByteArray();
            out.close();
            return serializedValue;
        } catch (Exception e) {
            throw new SerializationException("Error when serializing SojSession.", e);
        }
    }
}
