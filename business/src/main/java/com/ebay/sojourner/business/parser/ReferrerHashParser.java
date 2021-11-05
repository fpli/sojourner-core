package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class ReferrerHashParser implements FieldParser<RawEvent, UbiEvent> {

  private static final Logger log = Logger.getLogger(ReferrerHashParser.class);
  private static final String R_TAG = "r";

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    Long result = null;
    try {
      Map<String, String> map = new HashMap<>();
      map.putAll(rawEvent.getSojA());
      map.putAll(rawEvent.getSojK());
      map.putAll(rawEvent.getSojC());
      String reffererHash = null;
      if (StringUtils.isNotBlank(map.get(R_TAG))) {
        reffererHash = map.get(R_TAG);
      }

      if (StringUtils.isNotBlank(reffererHash)) {
        result = Long.parseLong(reffererHash);
        if (result < 999999999999999999L) {
          ubiEvent.setRefererHash(result);
        }
      }
    } catch (NumberFormatException e) {
      // log.debug("Parsing ReffererHash failed, format incorrect...");
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
