package com.ebay.sojourner.flink.connector.kafka.schema.serialize;

import com.ebay.sojourner.common.constant.KafkaMessageHeaders;
import com.ebay.sojourner.common.model.SojEvent;
import com.google.common.primitives.Ints;
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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SojEventKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<SojEvent> {

    private final String schemaRegistryUrl;
    private final String subjectName;
    private final String topic;

    private transient DatumWriter<RheosEvent> writer;
    private transient SchemaRegistryAwareAvroSerializerHelper<SojEvent> serializerHelper;
    private transient int schemaId;

    public SojEventKafkaRecordSerializationSchema(String schemaRegistryUrl, String subjectName, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.subjectName = subjectName;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        log.info("Initialize DatumWriter and SchemaRegistryAwareAvroSerializerHelper");
        this.writer = new GenericDatumWriter<>(SojEvent.getClassSchema());

        Map<String, Object> map = new HashMap<>();
        map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, schemaRegistryUrl);
        this.serializerHelper = new SchemaRegistryAwareAvroSerializerHelper<>(map, SojEvent.class);
        this.schemaId = serializerHelper.getSchemaId(subjectName);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SojEvent element, KafkaSinkContext context, Long timestamp) {
        List<Header> headers = new ArrayList<>();
        int pageId = element.getPageId() == null ? -1 : element.getPageId();
        Header pageIdHeader = new RecordHeader(KafkaMessageHeaders.PAGE_ID, Ints.toByteArray(pageId));
        headers.add(pageIdHeader);

        byte[] key = element.getGuid().getBytes(StandardCharsets.UTF_8);

        // convert SpecificRecord to GenericRecord
        GenericRecord record = GenericData.get().deepCopy(SojEvent.getClassSchema(), element);

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
            throw new SerializationException("Error when serializing SojEvent.", e);
        }

        return new ProducerRecord<>(topic, null, key, value, headers);
    }
}
