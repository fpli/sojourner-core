package com.ebay.sojourner.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode(of = {"metricType","metricName"})
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TdqConfigMapping {
  private String metricName;
  private MetricType metricType;
  private Set<Integer> pageIds;
  private Set<String> pageFamilys;
  private Set<String> tags;
  private String env;
}
