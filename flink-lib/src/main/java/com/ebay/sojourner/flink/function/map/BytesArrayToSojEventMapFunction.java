package com.ebay.sojourner.flink.function.map;

import com.ebay.sojourner.common.model.SojEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

@Slf4j
public class BytesArrayToSojEventMapFunction extends RichMapFunction<byte[], SojEvent> {

    private transient DatumReader<SojEvent> reader;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("Initialize SojEvent DatumReader.");
        reader = new SpecificDatumReader<>(SojEvent.class);
    }

    @Override
    public SojEvent map(byte[] data) throws Exception {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Error when deserializing SojEvent.", e);
        }
    }
}
