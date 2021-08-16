package com.ebay.sojourner.common.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RawSojEventWrapper {
  private String guid;
  private int pageId;
  private String topic;
  private byte[] payload;
  private Map<String, Long> timestamps;
  private Long eventTimestamp;
}
