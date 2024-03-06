package com.ebay.sojourner.flink.connector.kafka.schema.serialize;

import com.ebay.sojourner.common.model.BotSignature;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

@Slf4j
public class BotSignatureKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<BotSignature> {

    private final String topic;
    private final String keyField;

    private transient DatumWriter<BotSignature> writer;

    public BotSignatureKafkaRecordSerializationSchema(String topic, String keyField) {
        this.topic = topic;
        this.keyField = keyField;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        log.info("Initialize BotSignature DatumWriter.");
        this.writer = new SpecificDatumWriter<>(BotSignature.class);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(BotSignature element, KafkaSinkContext context, Long timestamp) {

        // ignore records with null key
        if (element.get(keyField) == null) {
            return null;
        }

        byte[] key = String.valueOf(element.get(keyField)).getBytes(StandardCharsets.UTF_8);

        // value bytes
        byte[] value = null;

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(element, encoder);
            encoder.flush();
            value = out.toByteArray();
            out.close();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing BotSignature.", e);
        }

        return new ProducerRecord<>(topic, null, key, value);
    }
}
