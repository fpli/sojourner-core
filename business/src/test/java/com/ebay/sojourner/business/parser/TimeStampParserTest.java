package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.business.util.LoadRawEventAndExpect;
import com.ebay.sojourner.business.util.ParserConstants;
import com.ebay.sojourner.business.util.TypeTransUtil;
import com.ebay.sojourner.business.util.VaildateResult;
import com.ebay.sojourner.business.util.YamlUtil;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TimeStampParserTest {

  private static final Logger logger = Logger.getLogger(TimeStampParserTest.class);

  private static UbiEvent ubiEvent = null;
  private static String parser = null;
  private static String caseItem = null;
  private static TimestampParser timestampParser = null;
  private static HashMap<String, Object> map = null;

  @BeforeAll
  public static void initParser() {
    parser = ParserConstants.TIMESTAMP;
    map = YamlUtil.getInstance().loadFileMap(ParserConstants.FILEPATH);
  }

  @Test
  public void testTimeStampParser1() {
    timestampParser = new TimestampParser();
    ubiEvent = new UbiEvent();
    caseItem = ParserConstants.CASE1;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        timestampParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), TypeTransUtil.LongToString(ubiEvent.getEventTimestamp())));
      }
    } catch (Exception e) {
      logger.error("timestamp test fail!!!");
    }
  }

  @Test
  public void testTimeStampParser2() {
    timestampParser = new TimestampParser();
    ubiEvent = new UbiEvent();
    caseItem = ParserConstants.CASE2;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        timestampParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), TypeTransUtil.LongToString(ubiEvent.getEventTimestamp())));
      }
    } catch (Exception e) {
      logger.error("timestamp test fail!!!");
    }
  }

  @Test
  public void testTimeStampParser3() {
    timestampParser = new TimestampParser();
    ubiEvent = new UbiEvent();
    caseItem = ParserConstants.CASE3;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        timestampParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), TypeTransUtil.LongToString(ubiEvent.getEventTimestamp())));
      }
    } catch (Exception e) {
      logger.error("timestamp test fail!!!");
    }
  }
}
