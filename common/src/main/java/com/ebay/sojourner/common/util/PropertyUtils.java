package com.ebay.sojourner.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class PropertyUtils {

  public static Properties loadInProperties(String absoluteFilePath, String alternativeResource)
      throws FileNotFoundException {
    Properties properties = new Properties();
    InputStream instream = null;
    try {
      instream = FileLoader.loadInStream(absoluteFilePath, alternativeResource);
      properties.load(instream);
    } catch (Exception e) {
      throw new FileNotFoundException(absoluteFilePath);
    } finally {
      if (instream != null) {
        try {
          instream.close();
        } catch (IOException e) {
        }
      }
    }

    return properties;
  }

  public static Properties loadInProperties(InputStream configFileStream)
      throws FileNotFoundException {
    Properties properties = new Properties();
    InputStream instream = null;
    try {
      instream = FileLoader.loadInStream(configFileStream);
      properties.load(instream);
    } catch (Exception e) {
      throw new FileNotFoundException("load file failed!!!");
    } finally {
      if (instream != null) {
        try {
          instream.close();
        } catch (IOException e) {
        }
      }
    }

    return properties;
  }

  /**
   * Return the values with sequential order by splitting property value with the delimiter
   */
  public static Collection<String> parseProperty(String property, String delimiter) {
    Collection<String> pageIdCollection = new ArrayList<String>();
    if (property != null) {
      String[] pageIds = property.split(delimiter);
      for (String pageId : pageIds) {
        pageIdCollection.add(pageId.trim());
      }
    }

    return pageIdCollection;
  }

  public static Set<Integer> getIntegerSet(String property, String delimiter) {
    HashSet<Integer> propertySet = new HashSet<Integer>();
    if (StringUtils.isNotBlank(property)) {
      String[] list = property.split(delimiter);
      for (int i = 0; i < list.length; i++) {
        propertySet.add(Integer.valueOf(list[i].trim()));
        // Do not ignore the NumberFormatException as it indicates the configuration errors.
      }
    }
    return propertySet;
  }

  public static Set<Long> getLongSet(String property, String delimiter) {
    HashSet<Long> propertySet = new HashSet<Long>();
    if (StringUtils.isNotBlank(property)) {
      String[] list = property.split(delimiter);
      for (int i = 0; i < list.length; i++) {
        propertySet.add(Long.valueOf(list[i].trim()));
        // Do not ignore the NumberFormatException as it indicates the configuration errors.
      }
    }
    return propertySet;
  }

  public static String mapToString(Map<String, String> sojMap) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> pair : sojMap.entrySet()) {
      sb.append(pair.getKey()).append("=").append(pair.getValue()).append("&");
    }
    if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  public static Map stringToMap(String sojStr) {
    if (StringUtils.isEmpty(sojStr)) {
      return null;
    }
    Map<String, String> sojMap = new LinkedHashMap<>();
    String[] keyValues = sojStr.split("&");
    if (keyValues != null && keyValues.length > 0) {
      for (String keyValue : keyValues) {
        String[] keyValuePair = keyValue.split("=",-1);
        if (keyValuePair != null && keyValuePair.length > 0) {
          if(keyValuePair.length==1){
            sojMap.put(keyValuePair[0], "");
          } else if(keyValuePair.length==2){
            sojMap.put(keyValuePair[0], keyValuePair[1]);
          } else{
            StringBuilder sb = new StringBuilder();
            for(int i=1;i<keyValuePair.length;i++){
              sb.append(keyValuePair[i]).append("=");
            }
            sojMap.put(keyValuePair[0], sb.substring(0,sb.length()-1));
          }

        }
      }
    }
    return sojMap;
  }

}
