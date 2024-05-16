package com.ebay.sojourner.flink.connector.kafka.schema.serialize;


import com.ebay.sojourner.common.model.RheosHeader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RheosHeaderValueSerializerSchema implements SerializationSchema<RheosHeader> {

    private transient DatumWriter<RheosHeader> writer;

    @Override
    public void open(InitializationContext context) throws Exception {
        this.writer = new SpecificDatumWriter<>(RheosHeader.class);
    }

    @Override
    public byte[] serialize(RheosHeader element) {
        byte[] serializedValue = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(element, encoder);
            encoder.flush();
            serializedValue = out.toByteArray();
            out.close();
        } catch (IOException e) {
            throw new SerializationException("Error when serializing RheosHeader.", e);
        }

        return serializedValue;
    }
}
