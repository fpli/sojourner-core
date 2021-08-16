package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.constant.SojHeaders;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SojTimestamp;
import java.util.HashMap;
import java.util.Map;

public class TimestampParser implements FieldParser<RawEvent, UbiEvent> {

  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) {
    Long eventTimestamp = rawEvent.getEventTimestamp();
    ubiEvent.setEventTimestamp(eventTimestamp);
    ubiEvent.setSojDataDt(SojTimestamp.castSojTimestampToDate(eventTimestamp));

    // Keep original session key from UBI Listener
    ubiEvent.setIngestTime(rawEvent.getIngestTime());
    Map<String, Long> timestamps = new HashMap<>();
    timestamps.put(SojHeaders.PATHFINDER_CREATE_TIMESTAMP,
        rawEvent.getRheosHeader().getEventCreateTimestamp());
    timestamps.put(SojHeaders.PATHFINDER_SENT_TIMESTAMP,
        rawEvent.getRheosHeader().getEventSentTimestamp());
    timestamps.put(SojHeaders.PATHFINDER_PRODUCER_TIMESTAMP,
        rawEvent.getTimestamps().get(SojHeaders.PATHFINDER_PRODUCER_TIMESTAMP));
    ubiEvent.setTimestamps(timestamps);
    ubiEvent.setOldSessionSkey(null);
  }

  @Override
  public void init() throws Exception {
  }
}
