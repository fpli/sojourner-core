package com.ebay.sojourner.flink.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor
@Getter
public enum DataCenter {

  @JsonProperty("rno")
  RNO("RNO"),

  @JsonProperty("slc")
  SLC("SLC"),

  @JsonProperty("lvs")
  LVS("LVS");

  private final String value;

  @Override
  public String toString() {
    return name().toLowerCase();
  }

  public static DataCenter of(String dataCenter) {
    Preconditions.checkArgument(StringUtils.isNotBlank(dataCenter));
    return DataCenter.valueOf(dataCenter.toUpperCase());
  }
}
