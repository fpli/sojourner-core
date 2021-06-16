package com.ebay.sojourner.common.env;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
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
      String value = properties.getProperty(key);
      if (value.startsWith("[") && value.endsWith("]")) {
        String substring = value.substring(1, value.length() - 1);
        String[] split = substring.split(",");
        List<String> listValues = new ArrayList<>();
        for (String s : split) {
          listValues.add(s.trim());
        }
        this.props.put(key, listValues);
      } else {
        this.props.put(key, value);
      }
    }
  }

  @Override
  public Integer order() {
    return 1;
  }
}
