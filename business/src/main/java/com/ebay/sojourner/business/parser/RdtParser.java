package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.util.SOJNVL;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class RdtParser implements FieldParser<RawEvent, UbiEvent> {

  public static final String RDT = "rdt";

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {

    String payload = ubiEvent.getApplicationPayload();

    try {
      if (StringUtils.isNotBlank(SOJNVL.getTagValue(payload, RDT))) {
        if ("0".equals(SOJNVL.getTagValue(payload, RDT))) {
          ubiEvent.setRdt(false);
        } else {
          ubiEvent.setRdt(true);
        }
      } else {
        ubiEvent.setRdt(false);
      }
    } catch (Exception e) {
      // log.error("Parsing rdt failed, format wrong...", e);
      ubiEvent.setRdt(false);
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
