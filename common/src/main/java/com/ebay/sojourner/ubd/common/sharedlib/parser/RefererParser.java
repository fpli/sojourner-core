package com.ebay.sojourner.ubd.common.sharedlib.parser;

import com.ebay.sojourner.ubd.common.model.RawEvent;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import org.apache.commons.lang3.StringUtils;

public class RefererParser implements FieldParser<RawEvent, UbiEvent> {

  @Override
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {
    String clientData = rawEvent.getClientData().getReferrer();
    if (StringUtils.isNotBlank(clientData)&&!clientData.equalsIgnoreCase("null")) {
      ubiEvent.setReferrer(clientData);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
