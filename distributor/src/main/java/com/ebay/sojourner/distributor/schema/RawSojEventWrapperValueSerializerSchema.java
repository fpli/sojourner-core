package com.ebay.sojourner.distributor.schema;


import com.ebay.sojourner.common.model.RawSojEventWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class RawSojEventWrapperValueSerializerSchema
    implements SerializationSchema<RawSojEventWrapper> {

  @Override
  public byte[] serialize(RawSojEventWrapper element) {
    return element.getValue();
  }
}
