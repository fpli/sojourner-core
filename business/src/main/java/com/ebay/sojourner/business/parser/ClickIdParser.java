package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.IntegerField;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ClickIdParser implements FieldParser<RawEvent, UbiEvent> {

  private static final String C_TAG = "c";

  @Override
  public void init() throws Exception {
  }

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    String clickId = null;
    Map<String, String> map = new HashMap<>();
    map.putAll(rawEvent.getSojA());
    map.putAll(rawEvent.getSojK());
    map.putAll(rawEvent.getSojC());
    if (StringUtils.isNotBlank(map.get(C_TAG))) {
      clickId = map.get(C_TAG);
    }
    long result = 0;
    try {
      String clickValue = IntegerField.parse(clickId);
      if (StringUtils.isNotBlank(clickValue)) {
        result = Long.parseLong(clickValue);
        result = result % 65536;
        if (result > 32767) {
          result -= 65536;
        }
        ubiEvent.setClickId((int) result);
      }
    } catch (NumberFormatException e) {
      // log.debug("Parsing ClickId failed, format incorrect: " + clickId);
    }
  }
}
