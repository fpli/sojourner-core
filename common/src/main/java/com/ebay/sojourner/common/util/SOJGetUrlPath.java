package com.ebay.sojourner.common.util;

import java.net.URI;
import java.net.URISyntaxException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class SOJGetUrlPath {

  /*
   * this function is to get path from a url string
   */
  public static String getUrlPath(String urlString) {
    if (StringUtils.isBlank(urlString)) {
      return "";
    }
    URI uri;

    try {
      uri = new URI(urlString);
    } catch (URISyntaxException e) {
      return "";
    }
    if(uri.getPath()==null){
      return "";
    }else{
      return uri.getPath();
    }
  }
}
