package com.ebay.sojourner.flink.common;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class KafkaHeaderUtils {

  /**
   * get Integer value from kafka message header by key
   * return null if the key does not present
   */
  public static Integer getInt(String key, Headers headers) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(headers);

    Header header = headers.lastHeader(key);
    if (header == null) return null;
    return Ints.fromByteArray(header.value());
  }

}
