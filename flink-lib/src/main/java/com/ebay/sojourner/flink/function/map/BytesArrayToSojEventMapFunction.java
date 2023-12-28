package com.ebay.sojourner.flink.function.map;

import com.ebay.sojourner.common.model.SojEvent;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

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
    Decoder decoder = null;
    try {
      decoder = DecoderFactory.get().binaryDecoder(data, null);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException("Deserialize SojEvent error", e);
    }
  }
}
