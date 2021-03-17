package com.ebay.sojourner.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode(of = "topic")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TopicPageIdMapping {
  private String topic;
  private Set<Integer> pageIds;
  private String profile;
}
