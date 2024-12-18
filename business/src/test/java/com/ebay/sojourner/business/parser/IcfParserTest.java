package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class IcfParserTest {

  private IcfParser icfParser;
  private RawEvent rawEvent;
  private UbiEvent ubiEvent;
  private Map<String, String> sojA;
  private Map<String, String> sojC;
  private Map<String, String> sojK;

  @BeforeEach
  public void setup() {
    icfParser = new IcfParser();
    rawEvent = new RawEvent();
    ubiEvent = new UbiEvent();
    sojA = new HashMap<>();
    sojC = new HashMap<>();
    sojK = new HashMap<>();
  }

  @Test
  @DisplayName("applicationPayload is null and icf is null")
  public void test_applicationPayload_is_null() throws Exception {
    rawEvent.setSojA(sojA);
    rawEvent.setSojC(sojC);
    rawEvent.setSojK(sojK);
    icfParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(0, ubiEvent.getIcfBinary());
  }

  @Test
  @DisplayName("applicationPayload is not null and icf is null")
  public void test_icf_is_null() throws Exception {
    sojA.put("icf", " ");
    rawEvent.setSojA(sojA);
    rawEvent.setSojC(sojC);
    rawEvent.setSojK(sojK);
    icfParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(0, ubiEvent.getIcfBinary());
  }

  @Test
  @DisplayName("applicationPayload is not null and icf is not null")
  public void test_icf_is_not_null() throws Exception {
    sojA.put("icf", "0");
    rawEvent.setSojA(sojA);
    rawEvent.setSojC(sojC);
    rawEvent.setSojK(sojK);
    icfParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(0, ubiEvent.getIcfBinary());
  }

  @Test
  public void test_sojA_and_sojK_is_null() throws Exception {
    sojC.put("icf", "0");
    rawEvent.setSojA(sojA);
    rawEvent.setSojC(sojC);
    rawEvent.setSojK(sojK);
    icfParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(0, ubiEvent.getIcfBinary());
  }

  @Test
  public void test_sojA_and_sojK_and_sojC_is_null() throws Exception {
    rawEvent.setSojA(sojA);
    rawEvent.setSojC(sojC);
    rawEvent.setSojK(sojK);
    icfParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(0, ubiEvent.getIcfBinary());
  }
}
