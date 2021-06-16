package com.ebay.sojourner.distributor.route;

import static com.ebay.sojourner.distributor.route.SojEventRouteRuleFactory.routeRules;

import com.ebay.sojourner.common.model.SojEvent;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Router for special topics, routing logic is not based on pageIds
 */
public class SojEventRouter implements Router<SojEvent> {

  private final Map<String, String> topicConfigMap;

  public SojEventRouter(Map<String, String> topicConfigMap) {
    this.topicConfigMap = topicConfigMap;
  }


  @Override
  public Set<String> target(SojEvent sojEvent) {
    Set<String> topics = Sets.newHashSet();
    if (topicConfigMap != null) {
      for (Entry<String, String> topicConfig : topicConfigMap.entrySet()) {
        String key = topicConfig.getKey();
        String topic = topicConfig.getValue();
        if (routeRules.containsKey(key)) {
          if (routeRules.get(key).match(sojEvent)) {
            topics.add(topic);
          }
        } else {
          throw new RuntimeException("Cannot find route rule for key: " + key);
        }
      }
    }
    return topics;
  }
}
