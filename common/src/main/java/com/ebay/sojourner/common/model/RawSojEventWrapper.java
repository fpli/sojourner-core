package com.ebay.sojourner.common.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RawSojEventWrapper {
  private String topic;
  private byte[] key;
  private byte[] value;
  private Map<String, Long> timestamps;
  private Long eventTimestamp;
}
