package com.ebay.sojourner.distributor.route;

import com.ebay.sojourner.distributor.route.rule.AbstractSojEventRouteRule;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.reflections.Reflections;

@Slf4j
public class SojEventRouteRuleFactory {

  public static final Map<String, AbstractSojEventRouteRule> routeRules = new HashMap<>();

  static {
    Reflections reflections = new Reflections("com.ebay.sojourner.distributor");
    Set<Class<?>> clazzes = reflections.getTypesAnnotatedWith(Route.class);
    for (Class<?> clazz : clazzes) {
      AbstractSojEventRouteRule routeRule = null;
      try {
        routeRule = (AbstractSojEventRouteRule) clazz.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Route annotation = routeRule.getClass().getAnnotation(Route.class);
      if (StringUtils.isBlank(annotation.key())) {
        throw new RuntimeException("Cannot find route rule key for class: " + clazz);
      }

      if (routeRules.get(annotation.key()) == null) {
        routeRules.put(annotation.key(), routeRule);
      } else {
        throw new RuntimeException("Duplicate route rule with key: " + annotation.key());
      }
    }

    log.info("Loaded {} route rules: {}", routeRules.entrySet().size(), routeRules.keySet());
  }
}
