package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.business.util.LoadRawEventAndExpect;
import com.ebay.sojourner.business.util.ParserConstants;
import com.ebay.sojourner.business.util.VaildateResult;
import com.ebay.sojourner.business.util.YamlUtil;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class IframeParserTest {

  private static UbiEvent ubiEvent = null;
  private static String parser = null;
  private static String caseItem = null;
  private static IFrameParser iFrameParser = null;
  private static HashMap<String, Object> map = null;

  @BeforeAll
  public static void initParser() {
    parser = ParserConstants.IFRAME;
    map = YamlUtil.getInstance().loadFileMap(ParserConstants.FILEPATH);
  }

  @Test
  public void testIframeParser1() {
    iFrameParser = new IFrameParser();
    ubiEvent = new UbiEvent();
    caseItem = ParserConstants.CASE1;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        iFrameParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(entry.getValue(), String.valueOf(ubiEvent.isIframe())));
      }
    } catch (Exception e) {
      log.error("iframe test fail!!!");
    }
  }

  @Test
  public void testIframeParser2() {
    iFrameParser = new IFrameParser();
    ubiEvent = new UbiEvent();
    caseItem = ParserConstants.CASE2;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        iFrameParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(entry.getValue(), String.valueOf(ubiEvent.isIframe())));
      }
    } catch (Exception e) {
      log.error("iframe test fail!!!");
    }
  }
}
