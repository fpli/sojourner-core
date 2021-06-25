package com.ebay.sojourner.common.env;

import java.util.Enumeration;
import java.util.Properties;

public class ArgsSource extends AbstractEnvironment {

  private final Properties properties;

  public ArgsSource(Properties properties) {
    this.properties = properties;
  }

  @Override
  public void sourceProps() {

    Enumeration<?> enumeration = properties.propertyNames();
    while (enumeration.hasMoreElements()) {
      String key = (String) enumeration.nextElement();
      this.props.put(key, properties.get(key));
    }
  }

  @Override
  public Integer order() {
    return 1;
  }
}
