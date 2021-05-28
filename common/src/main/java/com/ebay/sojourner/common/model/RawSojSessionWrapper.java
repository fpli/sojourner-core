package com.ebay.sojourner.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RawSojSessionWrapper {
  private byte[] key;
  private byte[] value;
}
