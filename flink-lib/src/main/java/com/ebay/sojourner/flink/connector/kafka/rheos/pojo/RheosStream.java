package com.ebay.sojourner.flink.connector.kafka.rheos.pojo;

import com.ebay.sojourner.flink.common.DataCenter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.apache.kafka.common.security.auth.SecurityProtocol;

@Data
public class RheosStream {

  @JsonProperty("stream")
  private String name;

  @JsonProperty("auth-protocol")
  private SecurityProtocol authProtocol;

  private Map<DataCenter, List<String>> brokers;
}
