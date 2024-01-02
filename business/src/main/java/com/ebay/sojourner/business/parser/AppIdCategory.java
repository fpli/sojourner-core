package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.PropertyUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AppIdCategory {

  public static final Set<Long> MOBILE_APP = new HashSet<Long>();
  public static final Set<Long> DESKTOP_APP = new HashSet<Long>();
  public static final Set<Long> EIM_APP = new HashSet<Long>();

  private Set<Integer> appIds = null;

  public AppIdCategory(String appIds) {
    this(parse(appIds));
  }

  public AppIdCategory(Set<Integer> appIds) {
    this.appIds = Collections.unmodifiableSet(appIds);
  }

  /*
  public static void initAppIdCategory(Configuration conf){
    parse(conf.get(Property.MOBILE_APP), MOBILE_APP);
    parse(conf.get(Property.DESKTOP_APP), DESKTOP_APP);
    parse(conf.get(Property.EIM_APP), EIM_APP);
  }
  */

  public static Set<Integer> parse(String appIds) {
    Set<Integer> appIdSet = new HashSet<Integer>();

    Collection<String> ids = PropertyUtils.parseProperty(appIds, Property.PROPERTY_DELIMITER);
    for (String id : ids) {
      try {
        appIdSet.add(Integer.valueOf(id.trim()));
      } catch (NumberFormatException e) {
        e.printStackTrace(System.err);
        log.error("Format page Id error: " + id);
      }
    }

    return appIdSet;
  }

  public boolean isCorrespondingAppId(UbiEvent event) {
    Integer appId = event.getAppId();
    return appId != null && appIds.contains(appId);
  }
}
