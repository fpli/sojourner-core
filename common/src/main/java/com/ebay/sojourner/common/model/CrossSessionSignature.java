package com.ebay.sojourner.common.model;

import java.io.Serializable;
import java.util.Set;
import lombok.Data;

@Data
public class CrossSessionSignature implements Serializable {

  private String signatureId;
  private String signatureValue;
  private Boolean isGenerate;
  private Long expirationTime;
  private Set<Integer> botFlags;
}
