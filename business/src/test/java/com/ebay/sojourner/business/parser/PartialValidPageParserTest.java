package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.business.util.LoadRawEventAndExpect;
import com.ebay.sojourner.business.util.ParserConstants;
import com.ebay.sojourner.business.util.VaildateResult;
import com.ebay.sojourner.business.util.YamlUtil;
import com.ebay.sojourner.common.model.ClientData;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class PartialValidPageParserTest {

  private static UbiEvent ubiEvent = null;
  private static ClientData clientData = null;
  private static String parser = null;
  private static String caseItem = null;
  private static PartialValidPageParser partialValidPageParser = null;
  private static HashMap<String, Object> map = null;

  @BeforeAll
  public static void initParser() {
    parser = ParserConstants.PARTIAL;
    map = YamlUtil.getInstance().loadFileMap(ParserConstants.FILEPATH);
  }

  @Test
  public void testPartialValidPageParser1() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setRdt(true);

    caseItem = ParserConstants.CASE1;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser2() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(3686);
    ubiEvent.setUrlQueryString("Portlet");

    caseItem = ParserConstants.CASE2;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser3() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(451);
    ubiEvent.setUrlQueryString("LogBuyerRegistrationJSEvent");

    caseItem = ParserConstants.CASE3;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser4() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setWebServer("sandbox.ebay.internet");

    caseItem = ParserConstants.CASE4;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser5() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2588);
    ubiEvent.setApplicationPayload("cflgs=ebay");

    caseItem = ParserConstants.CASE5;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser6() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(123);
    ubiEvent.setApplicationPayload("cflgs=ebay");

    caseItem = ParserConstants.CASE6;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser7() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setApplicationPayload("cflgs=ebay");

    caseItem = ParserConstants.CASE7;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser8() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setSqr("null");

    caseItem = ParserConstants.CASE8;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser9() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(1468660);
    ubiEvent.setSiteId(0);
    ubiEvent.setWebServer("rover.ebay.com");

    caseItem = ParserConstants.CASE9;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser10() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(1702440);
    ubiEvent.setWebServer("rover.ebay.haxi");

    caseItem = ParserConstants.CASE10;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser11() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setApplicationPayload("an=ebay&av=ebay");

    caseItem = ParserConstants.CASE11;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser12() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setApplicationPayload("in=ebay");

    caseItem = ParserConstants.CASE12;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser13() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(5360);
    ubiEvent.setUrlQueryString("ebay_xhr=2");

    caseItem = ParserConstants.CASE13;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      //            System.out.println(e.getMessage());
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser14() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setUrlQueryString("/_vti_bin");

    caseItem = ParserConstants.CASE14;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser15() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setApplicationPayload("mr=ebay");
    ubiEvent.setUrlQueryString("?redirect=mobile");

    caseItem = ParserConstants.CASE15;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser16() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2043141);
    ubiEvent.setUrlQueryString("/intercept.jsf");

    caseItem = ParserConstants.CASE16;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPartialValidPageParser17() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2765);
    ubiEvent.setApplicationPayload("stateebay");

    caseItem = ParserConstants.CASE17;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser18() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setUrlQueryString("_showdiag=1");

    caseItem = ParserConstants.CASE18;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser19() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();
    clientData = new ClientData();

    clientData.setRemoteIP("10.2.137.50");

    ubiEvent.setClientData(clientData);
    ubiEvent.setUrlQueryString("_showdiag=");

    caseItem = ParserConstants.CASE19;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser20() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setUrlQueryString("/&nbsb;");

    caseItem = ParserConstants.CASE20;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser21() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setSqr("postalCodeTestQuery");
    ubiEvent.setPageId(1677950);

    caseItem = ParserConstants.CASE21;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser22() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(5713);

    caseItem = ParserConstants.CASE22;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser23() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();
    clientData = new ClientData();

    clientData.setAgent("eBayNioHttpClient");

    ubiEvent.setClientData(clientData);
    ubiEvent.setPageId(Integer.MIN_VALUE);

    caseItem = ParserConstants.CASE23;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser24() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2050867);
    ubiEvent.setUrlQueryString("json");

    caseItem = ParserConstants.CASE24;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser25() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2052122);
    ubiEvent.setUrlQueryString("json");

    caseItem = ParserConstants.CASE25;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser26() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setUrlQueryString("null");

    caseItem = ParserConstants.CASE26;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser27() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2050601);
    ubiEvent.setPageName("ebayFeedHome");

    caseItem = ParserConstants.CASE27;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser28() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2054095);
    ubiEvent.setUrlQueryString("ebay/survey");

    caseItem = ParserConstants.CASE28;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser29() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2056116);
    ubiEvent.setUrlQueryString("/itm/watchInline");

    caseItem = ParserConstants.CASE29;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser30() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2059707);
    ubiEvent.setUrlQueryString("/itm/delivery");

    caseItem = ParserConstants.CASE30;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser31() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2052197);
    ubiEvent.setUrlQueryString("ImportHubItemDescription");

    caseItem = ParserConstants.CASE31;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser32() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2047935);
    ubiEvent.setWebServer("reco.ebay.haxi");

    caseItem = ParserConstants.CASE32;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser33() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2067339);
    ubiEvent.setUrlQueryString("/roverimp/0/0/9?");

    caseItem = ParserConstants.CASE33;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser34() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2056812);

    caseItem = ParserConstants.CASE34;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser35() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    ubiEvent.setPageId(2056116);

    caseItem = ParserConstants.CASE35;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser36() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();
    clientData = new ClientData();

    clientData.setAgent("ebayUserAgent/eBayIOS");

    ubiEvent.setClientData(clientData);
    ubiEvent.setPageId(2481888);
    ubiEvent.setAppId(3564);

    caseItem = ParserConstants.CASE36;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }

  @Test
  public void testPatialValidPageParser37() {
    partialValidPageParser = new PartialValidPageParser();
    ubiEvent = new UbiEvent();

    caseItem = ParserConstants.CASE37;

    try {
      HashMap<RawEvent, Object> rawEventAndExpectResult =
          LoadRawEventAndExpect.getRawEventAndExpect(map, parser, caseItem);
      for (Map.Entry<RawEvent, Object> entry : rawEventAndExpectResult.entrySet()) {
        partialValidPageParser.parse(entry.getKey(), ubiEvent);
        System.out.println(
            VaildateResult.validateString(
                entry.getValue(), String.valueOf(ubiEvent.isPartialValidPage())));
      }
    } catch (Exception e) {
      log.error("partial test fail!!!");
    }
  }
}
