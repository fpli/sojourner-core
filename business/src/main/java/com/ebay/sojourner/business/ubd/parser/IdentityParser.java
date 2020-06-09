package com.ebay.sojourner.business.ubd.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.PropertyUtils;
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
    String applicationPayload = null;
    String mARecString = PropertyUtils.mapToString(rawEvent.getSojA());
    String mKRecString = PropertyUtils.mapToString(rawEvent.getSojK());
    String mCRecString = PropertyUtils.mapToString(rawEvent.getSojC());
    if (mARecString != null) {
      applicationPayload = mARecString;
    }
    if ((applicationPayload != null) && (mKRecString != null)) {
      applicationPayload = applicationPayload + "&" + mKRecString;
    }

    // else set C record
    if (applicationPayload == null) {
      applicationPayload = mCRecString;
    }
    if (map.containsKey(G_TAG)) {
      ubiEvent.setGuid(map.get(G_TAG));
    }
    // ubiEvent.setGuid(rawEvent.getGuid());
    ubiEvent.setClientData(rawEvent.getClientData());
    ubiEvent.setUrlQueryString(rawEvent.getClientData().getUrlQueryString());
    ubiEvent.setApplicationPayload(applicationPayload);
    ubiEvent.setPageName(rawEvent.getClientData().getTName());
    ubiEvent.setVersion(Constants.EVENT_VERSION);
    //        ubiEvent.setConfiguration(configuration);
  }

  @Override
  public void init() throws Exception {
    //        configuration=context;

  }
}