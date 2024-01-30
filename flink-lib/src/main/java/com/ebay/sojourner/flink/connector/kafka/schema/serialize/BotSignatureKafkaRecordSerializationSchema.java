package com.ebay.sojourner.flink.connector.kafka.schema.serialize;

import com.ebay.sojourner.common.model.BotSignature;
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
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class BotSignatureKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<BotSignature> {

    private final String schemaRegistryUrl;
    private final String subjectName;
    private final String topic;

    private final String keyField;

    private transient DatumWriter<RheosEvent> writer;
    private transient SchemaRegistryAwareAvroSerializerHelper<BotSignature> serializerHelper;
    private transient int schemaId;

    public BotSignatureKafkaRecordSerializationSchema(String schemaRegistryUrl, String subjectName,
                                                      String topic, String keyField) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.subjectName = subjectName;
        this.keyField = keyField;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        log.info("Initialize BotSignature DatumWriter and SchemaRegistryAwareAvroSerializerHelper");

        this.writer = new GenericDatumWriter<>(BotSignature.getClassSchema());

        Map<String, Object> map = new HashMap<>();
        map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, schemaRegistryUrl);
        this.serializerHelper = new SchemaRegistryAwareAvroSerializerHelper<>(map, BotSignature.class);
        this.schemaId = serializerHelper.getSchemaId(subjectName);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(BotSignature element, KafkaSinkContext context, Long timestamp) {

        byte[] key = ((String) element.get(keyField)).getBytes(StandardCharsets.UTF_8);

        // convert SpecificRecord to GenericRecord
        GenericRecord record = GenericData.get().deepCopy(BotSignature.getClassSchema(), element);

        // assemble RheosEvent
        RheosEvent rheosEvent = new RheosEvent(record);
        rheosEvent.setEventCreateTimestamp(System.currentTimeMillis());
        rheosEvent.setEventSentTimestamp(System.currentTimeMillis());
        rheosEvent.setSchemaId(schemaId);
        rheosEvent.setProducerId("sojourner");

        // value bytes
        byte[] value = null;

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(rheosEvent, encoder);
            encoder.flush();
            value = out.toByteArray();
            out.close();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing BotSignature.", e);
        }

        return new ProducerRecord<>(topic, null, key, value);
    }
}
