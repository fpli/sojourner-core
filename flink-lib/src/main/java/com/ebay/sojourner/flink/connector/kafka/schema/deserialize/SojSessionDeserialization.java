package com.ebay.sojourner.flink.connector.kafka.schema.deserialize;

import com.ebay.sojourner.common.model.SojSession;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;

@Slf4j
public class SojSessionDeserialization implements KafkaRecordDeserializationSchema<SojSession> {

    private transient DatumReader<SojSession> reader;

    @Override
    public void open(InitializationContext context) throws Exception {
        log.info("Initialize SojSession DatumReader.");
        reader = new SpecificDatumReader<>(SojSession.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SojSession> out) throws IOException {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
            SojSession sojSession = reader.read(null, decoder);
            out.collect(sojSession);
        } catch (IOException e) {
            throw new SerializationException("Error when deserializing SojSession.", e);
        }
    }

    @Override
    public TypeInformation<SojSession> getProducedType() {
        return TypeInformation.of(SojSession.class);
    }
}
