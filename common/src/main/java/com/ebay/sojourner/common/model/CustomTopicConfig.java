package com.ebay.sojourner.common.model;

import java.util.HashSet;
import java.util.Set;
import lombok.Data;

@Data
public class CustomTopicConfig {
  private Set<Integer> pageIds = new HashSet<>();
}
