package com.ebay.sojourner.common.model;

import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(of = "pageId")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class PageIdTopicMapping {
  private Integer pageId;
  private Set<String> topics;
  private String profile;
}
