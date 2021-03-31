package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SojTimestamp;

public class TimestampParser implements FieldParser<RawEvent, UbiEvent> {

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    Long eventTimestamp = rawEvent.getEventTimestamp();
    ubiEvent.setEventTimestamp(eventTimestamp);
    ubiEvent.setSojDataDt(SojTimestamp.castSojTimestampToDate(eventTimestamp));

    // Keep original session key from UBI Listener
    ubiEvent.setIngestTime(rawEvent.getIngestTime());
    ubiEvent.setGenerateTime(rawEvent.getRheosHeader().getEventCreateTimestamp());
    ubiEvent.setOldSessionSkey(null);
  }

  @Override
  public void init() throws Exception {
  }
}
