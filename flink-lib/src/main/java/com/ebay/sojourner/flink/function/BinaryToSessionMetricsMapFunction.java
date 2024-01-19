package com.ebay.sojourner.flink.function;

import com.ebay.sojourner.common.model.SessionMetrics;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.io.IOException;

public class BinaryToSessionMetricsMapFunction extends RichMapFunction<byte[], SessionMetrics> {

  private transient DatumReader<SessionMetrics> reader;

  @Override
  public SessionMetrics map(byte[] data) throws Exception {
    if (reader == null) {
      reader = new SpecificDatumReader<>(SessionMetrics.class);
    }

    Decoder decoder = null;
    try {
      decoder = DecoderFactory.get().binaryDecoder(data, null);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException("Deserialize SojSession error", e);
    }
  }
}
