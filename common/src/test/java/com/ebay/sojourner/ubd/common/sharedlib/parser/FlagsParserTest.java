package com.ebay.sojourner.ubd.common.sharedlib.parser;

import com.ebay.sojourner.ubd.common.model.RawEvent;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import com.ebay.sojourner.ubd.common.util.LoadRawEventAndExpect;
import com.ebay.sojourner.ubd.common.util.ParserConstants;
import com.ebay.sojourner.ubd.common.util.VaildateResult;
import com.ebay.sojourner.ubd.common.util.YamlUtil;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FlagsParserTest {

  private static UbiEvent ubiEvent = null;
  private static String parser = null;
  private static String caseItem = null;
  private static FlagsParser flagsParser = null;
  private static HashMap<String, Object> map = null;

  @BeforeAll
  public static void initParser() {
    parser = ParserConstants.FLAGS;
    map = YamlUtil.getInstance().loadFileMap(ParserConstants.FILEPATH);
  }

  @Test
  public void testFlagsParser() {
    flagsParser = new FlagsParser();
    ubiEvent = new UbiEvent();
    caseItem = ParserConstants.CASE1;
    HashMap<RawEvent, Object> rawEventAndExpectResult =
        LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
    for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
      flagsParser.parse(entry.getKey(), ubiEvent);
      System.out.println(VaildateResult.validateString(entry.getValue(), ubiEvent.getFlags()));
    }
  }
}