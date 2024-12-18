package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import org.apache.commons.lang3.StringUtils;

public class RefererParser implements FieldParser<RawEvent, UbiEvent> {

  @Override
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {
    String clientData = rawEvent.getClientData().getReferrer();
    //    if (StringUtils.isNotBlank(clientData)&&!clientData.equalsIgnoreCase("null")) {
    if (StringUtils.isNotBlank(clientData)) {
      ubiEvent.setReferrer(clientData);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
