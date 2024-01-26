package com.ebay.sojourner.flink.connector.kafka.schema.deserialize;

import com.ebay.sojourner.common.model.SessionMetrics;
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

import java.io.IOException;

@Slf4j
public class SessionMetricsDeserialization implements KafkaRecordDeserializationSchema<SessionMetrics> {

    private transient DatumReader<SessionMetrics> reader;

    @Override
    public void open(InitializationContext context) throws Exception {
        log.info("Initialize SessionMetrics DatumReader.");
        reader = new SpecificDatumReader<>(SessionMetrics.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SessionMetrics> out) throws IOException {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
            SessionMetrics sessionMetrics = reader.read(null, decoder);
            out.collect(sessionMetrics);
        } catch (IOException e) {
            throw new SerializationException("Error when deserializing SessionMetrics.", e);
        }
    }

    @Override
    public TypeInformation<SessionMetrics> getProducedType() {
        return TypeInformation.of(SessionMetrics.class);
    }
}
