package com.ebay.sojourner.flink.function.map;

import com.ebay.sojourner.common.model.SessionMetrics;
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
public class BytesArrayToSessionMetricsMapFunction extends RichMapFunction<byte[], SessionMetrics> {

    private transient DatumReader<SessionMetrics> reader;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("Initialize SessionMetrics DatumReader.");
        reader = new SpecificDatumReader<>(SessionMetrics.class);
    }

    @Override
    public SessionMetrics map(byte[] data) throws Exception {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Error when deserializing SessionMetrics.", e);
        }
    }
}
