package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.SOJNVL;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class ReguParser implements FieldParser<RawEvent, UbiEvent> {

  public static final String REGU = "regU";
  private static final Logger log = Logger.getLogger(ReguParser.class);

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
