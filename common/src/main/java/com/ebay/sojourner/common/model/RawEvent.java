package com.ebay.sojourner.common.model;

import java.io.Serializable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawEvent implements Serializable {

  private RheosHeader rheosHeader;
  private Map<String, String> sojA;
  private Map<String, String> sojK;
  private Map<String, String> sojC;
  private ClientData clientData;
  private long ingestTime;
  private Long eventTimestamp;
  private long messageSize;

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
  }
}
