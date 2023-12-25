package com.ebay.sojourner.common.env;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnvironmentUtils {

  private static final Set<AbstractEnvironment> PROP_SOURCES =
      Sets.newTreeSet(Comparator.comparing(AbstractEnvironment::order));


  static {
    log.info("Load environment properties file");

    PROP_SOURCES.add(new EnvSource());
    PROP_SOURCES.add(new PropertySource());

    // source props
    for (AbstractEnvironment propSource : PROP_SOURCES) {
      propSource.sourceProps();
    }

  }

  public static void activateProfile(String profile) {
    Preconditions.checkNotNull(profile);

    String configFileName = "application-" + profile;
    PropertySource propertySource = new PropertySource(configFileName, 3);
    propertySource.sourceProps();
    PROP_SOURCES.add(propertySource);
  }

  public static void fromProperties(Properties properties) {
    Preconditions.checkNotNull(properties);
    ArgsSource argsSource = new ArgsSource(properties);
    argsSource.sourceProps();
    PROP_SOURCES.add(argsSource);
  }

  public static void sourceArgProps(Properties properties) {
    fromProperties(properties);
  }

  public static String get(String key) {
    Preconditions.checkNotNull(key);
    for (AbstractEnvironment propSource : PROP_SOURCES) {
      if (propSource.contains(key)) {
        return propSource.getProperty(key);
      }
    }
    throw new ConfigNotFoundException("Cannot find property " + key);
  }

  public static String getString(String key) {
    return get(key);
  }

  public static String getOrNull(String key) {
    return getStringOrNull(key);
  }

  public static String getStringOrNull(String key) {
    return getStringOrDefault(key, null);
  }

  public static String getStringOrDefault(String key, String defaultValue) {
    String result;

    try {
      result = get(key);
    } catch (ConfigNotFoundException e) {
      return defaultValue;
    }

    if (result == null) {
      return defaultValue;
    }

    return result;
  }

  public static String[] getStringArray(String key, String delimiter) {
    String s = get(key);
    delimiter = "\\s*" + delimiter + "\\s*";
    return s.split(delimiter);
  }

  public static String[] getStringArray(String key) {
    return getStringArray(key, ",");
  }

  public static List<String> getStringList(String key, String delimiter) {
    String[] stringArray = getStringArray(key, delimiter);
    return Lists.newArrayList(stringArray);
  }

  public static List<String> getStringList(String key) {
    return getStringList(key, ",");
  }


  public static Boolean getBoolean(String key) {
    String booleanVal = get(key);
    return Boolean.valueOf(booleanVal);
  }

  public static Boolean getBooleanOrDefault(String key, boolean defaultValue) {
    String booleanVal;
    try {
      booleanVal = get(key);
    } catch (ConfigNotFoundException e) {
      return defaultValue;
    }

    return Boolean.valueOf(booleanVal);
  }

  public static Integer getInteger(String key) {
    String intVal = get(key);
    return Integer.valueOf(intVal);
  }

  public static Integer getIntegerOrDefault(String key, Integer defaultValue) {
    Integer result;

    try {
      result = getInteger(key);
    } catch (ConfigNotFoundException e) {
      return defaultValue;
    }

    return result;
  }

  public static Long getLong(String key) {
    String longVal = get(key);
    return Long.valueOf(longVal);
  }

  public static Long getLongOrDefault(String key, Long defaultValue) {
    Long result;

    try {
      result = getLong(key);
    } catch (ConfigNotFoundException e) {
      return defaultValue;
    }

    return result;
  }

  public static <T> T getForClass(String key, Class<T> clazz) {
    for (AbstractEnvironment propSource : PROP_SOURCES) {
      if (propSource.contains(key)) {
        return propSource.getProperty(key, clazz);
      }
    }
    throw new ConfigNotFoundException("Cannot find property " + key);
  }

  public static boolean isSet(String key) {
    for (AbstractEnvironment propSource : PROP_SOURCES) {
      if (propSource.contains(key)) {
        return true;
      }
    }
    return false;
  }
}
