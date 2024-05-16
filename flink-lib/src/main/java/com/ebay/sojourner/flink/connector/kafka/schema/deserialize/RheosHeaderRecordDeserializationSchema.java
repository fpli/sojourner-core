package com.ebay.sojourner.flink.connector.kafka.schema.deserialize;

import com.ebay.sojourner.common.model.RheosHeader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

public class RheosHeaderRecordDeserializationSchema implements KafkaRecordDeserializationSchema<RheosHeader> {

    private transient DatumReader<RheosHeader> reader;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        reader = new SpecificDatumReader<>(RheosHeader.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RheosHeader> out) throws IOException {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
            RheosHeader rheosHeader = reader.read(null, decoder);
            out.collect(rheosHeader);
        } catch (IOException e) {
            throw new SerializationException("Error when deserializing RheosHeader.", e);
        }
    }

    @Override
    public TypeInformation<RheosHeader> getProducedType() {
        return TypeInformation.of(RheosHeader.class);
    }
}
