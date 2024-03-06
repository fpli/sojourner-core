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
public class StaticPageTypeParserTest {

  private static UbiEvent ubiEvent = null;
  private static String parser = null;
  private static String caseItem = null;
  private static StaticPageTypeParser staticPageTypeParser = null;
  private static HashMap<String, Object> map = null;

  @BeforeAll
  public static void initParser() {
    parser = ParserConstants.STATICPAGE;
    map = YamlUtil.getInstance().loadFileMap(ParserConstants.FILEPATH);
  }

  // rdt and pageId not null,itemId not null,vtNewIdsMap not contains pageId
  @Test
  public void testStaticPageTypeParser1() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(123);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(789L);
    caseItem = ParserConstants.CASE1;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // rdt and pageId not null,vtNewIdsMap contains pageId,itemId is null,lkup is default,flags not
  // null
  @Test
  public void testStaticPageTypeParser2() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(4737);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(null);
    caseItem = ParserConstants.CASE2;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // rdt and pageId is null,other not care
  @Test
  public void testStaticPageTypeParser3() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(Integer.MIN_VALUE);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(null);
    caseItem = ParserConstants.CASE3;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 284,itemId is null
  @Test
  public void testStaticPageTypeParser4() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(284);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(null);
    caseItem = ParserConstants.CASE4;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 284,itemId not null
  @Test
  public void testStaticPageTypeParser5() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(284);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(1L);
    caseItem = ParserConstants.CASE5;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 5414,itemId not null
  @Test
  public void testStaticPageTypeParser6() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(5414);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(1L);
    caseItem = ParserConstants.CASE6;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 4018,itemId is not null
  @Test
  public void testStaticPageTypeParser7() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(4018);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(1L);
    caseItem = ParserConstants.CASE7;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 4018,itemId is null
  @Test
  public void testStaticPageTypeParser8() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(4018);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(null);
    caseItem = ParserConstants.CASE8;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 3994,itemId not null
  @Test
  public void testStaticPageTypeParser9() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(3994);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(2L);
    caseItem = ParserConstants.CASE9;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // rdt and pageId not null,vtNewIdsMap contains pageId,pageId is 1637,itemId is null,lkup is
  // default,flags is null
  @Test
  public void testStaticPageTypeParser10() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(4737);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("");
    ubiEvent.setItemId(null);
    caseItem = ParserConstants.CASE10;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // rdt and pageId not null,vtNewIdsMap contains pageId,pageId is 4737,itemId is null,lkup is
  // default,flags length is 46
  @Test
  public void testStaticPageTypeParser11() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(4737);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("1234567890123456789012345678901234567890abcdefg");
    ubiEvent.setItemId(null);
    caseItem = ParserConstants.CASE11;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // rdt and pageId not null,vtNewIdsMap contains pageId,pageId is 1637,itemId is null,lkup is
  // default,flags length is 46
  @Test
  public void testStaticPageTypeParser12() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(1637);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("1234567890123$56789012345678901234567890abcdzfg");
    ubiEvent.setItemId(null);
    caseItem = ParserConstants.CASE12;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 3994,itemId not null
  @Test
  public void testStaticPageTypeParser13() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(2499);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("ebay");
    ubiEvent.setItemId(2L);
    caseItem = ParserConstants.CASE13;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }

  // pageId is 5414,itemId not null,rdt is 1
  @Test
  public void testStaticPageTypeParser14() {
    staticPageTypeParser = new StaticPageTypeParser();
    ubiEvent = new UbiEvent();
    ubiEvent.setPageId(3897);
    ubiEvent.setRdt(true);
    ubiEvent.setFlags("haxiboeBay");
    ubiEvent.setItemId(1L);
    caseItem = ParserConstants.CASE14;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        staticPageTypeParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateInteger(entry.getValue(), ubiEvent.getStaticPageType()));
      }
    } catch (Exception e) {
      log.error("staticpage test fail!!!");
    }
  }
}
