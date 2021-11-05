package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class FlagsParser implements FieldParser<RawEvent, UbiEvent> {

  private static final Logger log = Logger.getLogger(FlagsParser.class);
  private static final String FLGS_TAG = "flgs";

  public void parse(RawEvent event, UbiEvent ubiEvent) {
    Map<String, String> map = new HashMap<>();
    map.putAll(event.getSojA());
    map.putAll(event.getSojK());
    map.putAll(event.getSojC());
    String flags = map.get(FLGS_TAG);
    if (StringUtils.isNotBlank(flags)) {
      try {
        ubiEvent.setFlags(flags.trim());
      } catch (NumberFormatException e) {
        // log.debug("Flag format is incorrect");
      }
    }
  }

  @Override
  public void init() throws Exception {
    // nothing to do
  }
}
