package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.IntegerField;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SiteIdParser implements FieldParser<RawEvent, UbiEvent> {

  private static final Logger log = LoggerFactory.getLogger(SiteIdParser.class);
  private static final String T_TAG = "t";

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    try {
      String siteId = null;
      Map<String, String> map = new HashMap<>();
      map.putAll(rawEvent.getSojA());
      map.putAll(rawEvent.getSojK());
      map.putAll(rawEvent.getSojC());
      if (StringUtils.isNotBlank(map.get(T_TAG))) {
        siteId = map.get(T_TAG);
      }
      siteId = IntegerField.parse(siteId);
      if (StringUtils.isNotBlank(siteId)) {
        ubiEvent.setSiteId(Integer.parseInt(siteId));
      }
    } catch (Exception e) {
      // log.debug("Parsing SiteId failed, format wrong...");
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
