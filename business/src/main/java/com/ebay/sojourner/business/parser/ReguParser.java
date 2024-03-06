package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SOJNVL;
import org.apache.commons.lang3.StringUtils;

public class ReguParser implements FieldParser<RawEvent, UbiEvent> {

  public static final String REGU = "regU";

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws RuntimeException {
    try {
      String regu = SOJNVL.getTagValue(ubiEvent.getApplicationPayload(), REGU);
      if (StringUtils.isNotBlank(regu)) {
        ubiEvent.setRegu(1);
      } else {
        ubiEvent.setRegu(0);
      }
    } catch (Exception e) {
      // log.debug("Parsing regu failed, format incorrect");
      ubiEvent.setRegu(0);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
