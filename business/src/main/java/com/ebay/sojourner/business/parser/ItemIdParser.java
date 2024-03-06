package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.IntegerField;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ItemIdParser implements FieldParser<RawEvent, UbiEvent> {

  private static final String _ITM_TAG = "_itm";
  private static final String ITM_TAG = "itm";

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    Map<String, String> map = new HashMap<>();
    map.putAll(rawEvent.getSojA());
    map.putAll(rawEvent.getSojK());
    map.putAll(rawEvent.getSojC());
    String itemId1 = null;
    String itemId2 = null;
    if (StringUtils.isNotBlank(map.get(_ITM_TAG))) {
      itemId1 = map.get(_ITM_TAG);
    }
    if (StringUtils.isNotBlank(map.get(ITM_TAG))) {
      itemId2 = map.get(ITM_TAG);
    }
    Long itemId = null;
    if (IntegerField.getIntVal(itemId1) != null) {
      try {
        itemId = Long.parseLong(itemId1.trim());
      } catch (Exception e) {
        // log.info("Parsing ItemId failed, format incorrect: " + itemId1);
      }
    } else {
      if (IntegerField.getIntVal(itemId2) != null) {
        try {
          itemId = Long.parseLong(itemId2.trim());
        } catch (Exception e) {
          // log.info("Parsing ItemId failed, format incorrect: " + itemId2);
        }
      }
    }

    if (itemId != null && itemId < 999999999999999999L) {
      ubiEvent.setItemId(itemId);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
