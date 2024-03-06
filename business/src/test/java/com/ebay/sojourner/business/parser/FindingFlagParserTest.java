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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class FindingFlagParserTest {

  private static UbiEvent ubiEvent = null;
  private static String parser = null;
  private static String caseItem = null;
  private static FindingFlagParser findingFlagParser = null;
  private static HashMap<String, Object> map = null;

  @BeforeAll
  public static void initParser() {
    parser = ParserConstants.FINDINGFLAG;
    map = YamlUtil.getInstance().loadFileMap(ParserConstants.FILEPATH);
  }

  @Test
  public void testFindingFlagParser1() {
    findingFlagParser = new FindingFlagParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(3141);
    caseItem = ParserConstants.CASE1;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        findingFlagParser.parse(entry.getKey(), ubiEvent);
        System.out.println(VaildateResult.validateInteger(entry.getValue(), ubiEvent.getBitVal()));
      }
    } catch (Exception e) {
      log.error("findingflag test fail!!!");
    }
  }

  @Test
  public void testFindingFlagParser2() {
    findingFlagParser = new FindingFlagParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(123);
    caseItem = ParserConstants.CASE2;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        findingFlagParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), TypeTransUtil.IntegerToString(ubiEvent.getBitVal())));
      }
    } catch (Exception e) {
      log.error("findingflag test fail!!!");
    }
  }
}
