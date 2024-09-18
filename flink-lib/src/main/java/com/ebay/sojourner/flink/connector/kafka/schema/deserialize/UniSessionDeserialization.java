package com.ebay.sojourner.flink.connector.kafka.schema.deserialize;

import com.ebay.sojourner.common.model.UniSession;
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

import java.io.IOException;

@Slf4j
public class UniSessionDeserialization implements KafkaRecordDeserializationSchema<UniSession> {

    private transient DatumReader<UniSession> reader;

    @Override
    public void open(InitializationContext context) throws Exception {
        log.info("Initialize UniSession DatumReader.");
        reader = new SpecificDatumReader<>(UniSession.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UniSession> out) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
        UniSession uniSession = reader.read(null, decoder);
        out.collect(uniSession);
    }

    @Override
    public TypeInformation<UniSession> getProducedType() {
        return TypeInformation.of(UniSession.class);
    }
}
