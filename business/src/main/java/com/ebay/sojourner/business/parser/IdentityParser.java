package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Constants;
import java.util.HashMap;
import java.util.Map;

public class IdentityParser implements FieldParser<RawEvent, UbiEvent> {

  private static final String G_TAG = "g";

  @Override
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {
    Map<String, String> map = new HashMap<>();
    map.putAll(rawEvent.getSojA());
    map.putAll(rawEvent.getSojK());
    map.putAll(rawEvent.getSojC());

    if (map.containsKey(G_TAG)) {
      ubiEvent.setGuid(map.get(G_TAG));
    }

    ubiEvent.setClientData(rawEvent.getClientData());
    ubiEvent.setUrlQueryString(rawEvent.getClientData().getUrlQueryString());
    ubiEvent.setPageName(rawEvent.getClientData().getTName());
    ubiEvent.setVersion(Constants.EVENT_VERSION);
  }

  @Override
  public void init() throws Exception {
  }
}
