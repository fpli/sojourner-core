package com.ebay.sojourner.common.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SimpleDistSojEventWrapper {

  private String guid;
  private String colo;
  private String topic;
  private byte[] value;
  private byte[] key;

}
