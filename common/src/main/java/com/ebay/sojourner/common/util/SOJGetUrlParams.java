package com.ebay.sojourner.common.util;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;

public class SOJGetUrlParams {

  public static String getUrlParams(String url) {
    if (StringUtils.isBlank(url)) {
      return "";
    }

    // test valid url
    try {
      URI uri = new URI(url);
    } catch (URISyntaxException e) {
      // if not valid url, return null
      return "";
    }

    // find char "?"
    int param_start_pos = url.indexOf("?");
    if (param_start_pos > 0) {
      return url.substring(++param_start_pos);
    } else {
      return "";
    }
  }
}
