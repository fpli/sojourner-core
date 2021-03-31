package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.RheosHeader;
import com.ebay.sojourner.common.model.UbiEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TimestampParserSpecialTest {

  private TimestampParser timestampParser;
  private UbiEvent ubiEvent;
  private RawEvent rawEvent;
  private RheosHeader rheosHeader;

  @BeforeEach
  public void setup() {
    timestampParser = new TimestampParser();
    ubiEvent = new UbiEvent();
    rawEvent = new RawEvent();
    rheosHeader = new RheosHeader();
  }

  @Test
  @DisplayName("ubievent timestamp is not null")
  public void test1() {
    rheosHeader.setEventCreateTimestamp(1599726436000L);
    rawEvent.setRheosHeader(rheosHeader);
    rawEvent.setIngestTime(1599726436000L);
    rawEvent.setEventTimestamp(1599726436000L);
    timestampParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(1599726436000L, ubiEvent.getEventTimestamp());
    Assertions.assertEquals(1555200000000L, ubiEvent.getSojDataDt());
    Assertions.assertEquals(1599726436000L, ubiEvent.getIngestTime());
    Assertions.assertEquals(1599726436000L, ubiEvent.getGenerateTime());
    Assertions.assertNull(ubiEvent.getOldSessionSkey());
  }
}
