package com.ebay.sojourner.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RawSojEventWrapper {
  private String guid;
  private int pageId;
  private int bot;
  private RawSojEventHeader headers;
  private String topic;
  private byte[] payload;
}
