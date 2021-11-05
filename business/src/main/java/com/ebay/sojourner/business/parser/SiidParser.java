package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.SOJBase64ToLong;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SOJURLDecodeEscape;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class SiidParser implements FieldParser<RawEvent, UbiEvent> {

  private static final Logger log = Logger.getLogger(SiidParser.class);
  private static final String SIID_TAG = "siid";

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    String siid = null;
    Map<String, String> map = new HashMap<>();
    map.putAll(rawEvent.getSojA());
    map.putAll(rawEvent.getSojK());
    map.putAll(rawEvent.getSojC());
    if (StringUtils.isNotBlank(map.get(SIID_TAG))) {
      siid = map.get(SIID_TAG);
    }
    try {
      if (siid != null) {
        String decodeSiid = SOJURLDecodeEscape.decodeEscapes(siid.trim(), '%');
        if (StringUtils.isNotBlank(decodeSiid)) {
          ubiEvent.setSourceImprId(SOJBase64ToLong.getLong(decodeSiid));
        }
      }
    } catch (Exception e) {
      // log.error("Parsing Ciid failed, the siid value is: " + siid);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
