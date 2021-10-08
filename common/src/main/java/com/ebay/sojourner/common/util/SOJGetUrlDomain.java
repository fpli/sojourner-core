package com.ebay.sojourner.common.util;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;

public class SOJGetUrlDomain {

  /*
   * this function is to get domain/host from a url string
   */
  public static String getUrlDomain(String urlString) {
    if (StringUtils.isBlank(urlString)) {
      return "";
    }

    URI uri;

    try {
      uri = new URI(urlString);
    } catch (URISyntaxException e) {
      return "";
    }

    return uri.getHost();
  }
}
