package com.ebay.sojourner.common.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RawSojEventWrapper {
  private String guid;
  private int pageId;
  private int bot;
  private Map<String, byte[]> headers;
  private String topic;
  private byte[] payload;
}
