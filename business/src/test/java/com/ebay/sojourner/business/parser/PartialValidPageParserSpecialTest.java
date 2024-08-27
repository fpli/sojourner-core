package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.ClientData;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

public class PartialValidPageParserSpecialTest {

  private PartialValidPageParser partialValidPageParser;
  private RawEvent rawEvent;
  private UbiEvent ubiEvent;

  @BeforeEach
  public void setup() {
    partialValidPageParser = new PartialValidPageParser();
    rawEvent = new RawEvent();
    ubiEvent = new UbiEvent();
  }

  @ParameterizedTest
  @DisplayName("cases on sqr")
  @ValueSource(strings = {"undefined", "ebay.htm", "ebay.asp", "ebay.jsp", "ebay.gif", "ebay.png",
      "ebay.pdf", "ebay.html", "ebay.php", "ebay.cgi", "ebay.jpeg", "ebay.swf", "ebay.txt",
      "ebay.wav", "ebay.zip", "ebay.flv", "ebay.dll", "ebay.ico", "ebay.jpg", "ebay.hideoutput"})
  public void test_sqr(String input) throws Exception {
    ubiEvent.setSqr(input);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @ParameterizedTest
  @DisplayName("cases on urlQueryString")
  @ValueSource(strings = {"undefined", "ebay.gif", "ebay.png", "ebay.pdf", "ebay.jpeg", "ebay.swf",
      "ebay.txt", "ebay.wav", "ebay.zip", "ebay.flv", "ebay.ico", "ebay.jpg"})
  public void test_urlQueryString(String input) throws Exception {
    ubiEvent.setUrlQueryString(input);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_and_urlQueryString_is_null() throws Exception {
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @ParameterizedTest
  @DisplayName("cases on pageId")
  @ValueSource(ints = {3686, 451})
  public void test_pageId_is_not_null_and_urlQueryString_is_null(int input) throws Exception {
    ubiEvent.setPageId(input);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_3686_and_urlQueryString_not_contains_Portlet() throws Exception {
    ubiEvent.setPageId(3686);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_451_and_urlQueryString_not_contains_LogBuyerRegistrationJSEvent()
      throws Exception {
    ubiEvent.setPageId(451);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_not_null_and_cflags_is_null()
      throws Exception {
    ubiEvent.setPageId(3030);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_1468660_and_webserver_is_not_rover_ebay()
      throws Exception {
    ubiEvent.setPageId(1468660);
    ubiEvent.setSiteId(0);
    ubiEvent.setWebServer("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_1468660_and_webserver_is_null()
      throws Exception {
    ubiEvent.setPageId(1468660);
    ubiEvent.setSiteId(0);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_not_null_and_webserver_is_not_start_with_rover_ebay()
      throws Exception {
    ubiEvent.setPageId(1702440);
    ubiEvent.setSiteId(0);
    ubiEvent.setWebServer("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_applicationPayload_is_not_contains_av()
      throws Exception {
    ubiEvent.setApplicationPayload("an=ebay");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("an", "ebay");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_5360_and_urlQueryString_is_null()
      throws Exception {
    ubiEvent.setPageId(5360);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_5360_and_urlQueryString_is_not_contains__xhr_2()
      throws Exception {
    ubiEvent.setPageId(5360);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_5360_and_urlQueryString_is_start_with_MSOffice()
      throws Exception {
    ubiEvent.setUrlQueryString("/MSOffice/cltreq.asp");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_mr_is_1_and_urlQueryString_contains_redirect_mobile()
      throws Exception {
    ubiEvent.setApplicationPayload("mr=1");
    ubiEvent.setUrlQueryString("&redirect=mobile");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("mr", "1");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2043141_and_urlQueryString_is_null()
      throws Exception {
    ubiEvent.setPageId(2043141);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2043141_and_urlQueryString_is_start_with_jsf()
      throws Exception {
    ubiEvent.setPageId(2043141);
    ubiEvent.setUrlQueryString("jsf.js");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2043141_and_urlQueryString_is_not_start_with_jsf_and_intercept()
      throws Exception {
    ubiEvent.setPageId(2043141);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_not_null_and_urlQueryString_is_not_start_with_jsf_and_intercept()
      throws Exception {
    ubiEvent.setPageId(2765);
    ubiEvent.setApplicationPayload("state=ebay");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("state", "ebay");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_remoteIp_is_not_in_iplists()
      throws Exception {
    ClientData clientData = new ClientData();
    clientData.setRemoteIP("127.0.0.1");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_urlQueryString_is_nbsp()
      throws Exception {
    ubiEvent.setUrlQueryString("/&nbsp;");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_1677950_and_sqr_is_null()
      throws Exception {
    ubiEvent.setPageId(1677950);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_1677950_and_sqr_is_not_null()
      throws Exception {
    ubiEvent.setPageId(1677950);
    ubiEvent.setSqr(":postalCodeTestQuery");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_5713_and_sojpage_is_null()
      throws Exception {
    ubiEvent.setPageId(5713);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_5713_and_sojpage_is_in_pagelists()
      throws Exception {
    ubiEvent.setPageId(5713);
    ubiEvent.setApplicationPayload("page=cyp");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("page", "cyp");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_5713_and_sojpage_is_not_in_pagelists()
      throws Exception {
    ubiEvent.setPageId(5713);
    ubiEvent.setApplicationPayload("page=ebay");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("page", "ebay");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_1000()
      throws Exception {
    ubiEvent.setPageId(1000);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_null_and_agentString_is_not_null()
      throws Exception {
    ClientData clientData = new ClientData();
    clientData.setAgent("eBayNioHttpClient");
    ubiEvent.setClientData(clientData);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050757_and_agentString_is_not_null()
      throws Exception {
    ClientData clientData = new ClientData();
    clientData.setAgent("eBayNioHttpClient");
    ubiEvent.setClientData(clientData);
    ubiEvent.setPageId(2050757);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050867_and_urlQueryString_is_null()
      throws Exception {
    ubiEvent.setPageId(2050867);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050867_and_urlQueryString_is_not_null1()
      throws Exception {
    ubiEvent.setPageId(2050867);
    ubiEvent.setUrlQueryString("/local/availability");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050867_and_urlQueryString_is_not_null2()
      throws Exception {
    ubiEvent.setPageId(2050867);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050519_and_urlQueryString_contains_json()
      throws Exception {
    ubiEvent.setPageId(2050519);
    ubiEvent.setUrlQueryString("ebay.json");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050757()
      throws Exception {
    ubiEvent.setPageId(2050757);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2052122_and_urlQueryString_not_contains_json()
      throws Exception {
    ubiEvent.setPageId(2052122);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050601_and_pagename_is_null()
      throws Exception {
    ubiEvent.setPageId(2050601);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2050601_and_pagename_is_not_null()
      throws Exception {
    ubiEvent.setPageId(2050601);
    ubiEvent.setPageName("FeedHome");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2054095_and_urlQueryString_is_null()
      throws Exception {
    ubiEvent.setPageId(2054095);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2054095_and_urlQueryString_is_not_null()
      throws Exception {
    ubiEvent.setPageId(2054095);
    ubiEvent.setUrlQueryString("/survey");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056116_and_urlQueryString_is_not_null1()
      throws Exception {
    ubiEvent.setPageId(2056116);
    ubiEvent.setUrlQueryString("/itm/ajaxSmartAppBanner");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056116_and_urlQueryString_is_not_null2()
      throws Exception {
    ubiEvent.setPageId(2056116);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2059707_and_urlQueryString_is_null()
      throws Exception {
    ubiEvent.setPageId(2059707);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2059707_and_urlQueryString_is_not_null()
      throws Exception {
    ubiEvent.setPageId(2059707);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2052197_and_urlQueryString_is_null()
      throws Exception {
    ubiEvent.setPageId(2052197);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2052197_and_urlQueryString_is_not_null1()
      throws Exception {
    ubiEvent.setPageId(2052197);
    ubiEvent.setUrlQueryString("ImportHubCreateListing");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2052197_and_urlQueryString_is_not_null2()
      throws Exception {
    ubiEvent.setPageId(2052197);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2053898()
      throws Exception {
    ubiEvent.setPageId(2053898);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2053898_and_webserver_is_not_null()
      throws Exception {
    ubiEvent.setPageId(2053898);
    ubiEvent.setWebServer("EBAY");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2067339_and_urlQueryString_is_null()
      throws Exception {
    ubiEvent.setPageId(2067339);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2067339_and_urlQueryString_is_not_null()
      throws Exception {
    ubiEvent.setPageId(2067339);
    ubiEvent.setUrlQueryString("ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056116()
      throws Exception {
    ubiEvent.setPageId(2056116);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056116_and_pfn_is_not_null1()
      throws Exception {
    ubiEvent.setPageId(2056116);
    ubiEvent.setApplicationPayload("pfn=ebay");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("pfn", "ebay");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056116_and_pfn_is_not_null2()
      throws Exception {
    ubiEvent.setPageId(2056116);
    ubiEvent.setApplicationPayload("pfn=VI");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("pfn", "VI");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_10001()
      throws Exception {
    ubiEvent.setPageId(10001);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056812_and_sojpage_is_not_null1()
      throws Exception {
    ubiEvent.setPageId(2056812);
    ubiEvent.setApplicationPayload("page=ryprender");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("page", "ryprender");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056812_and_sojpage_is_not_null2()
      throws Exception {
    ubiEvent.setPageId(2056812);
    ubiEvent.setApplicationPayload("page=cyprender");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("page", "cyprender");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2056812_and_sojpage_is_not_null3()
      throws Exception {
    ubiEvent.setPageId(2056812);
    ubiEvent.setApplicationPayload("page=eabay");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("page", "eabay");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2481888_and_appid_is_not_3564()
      throws Exception {
    ubiEvent.setPageId(2481888);
    ubiEvent.setAppId(1565);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2481888_and_appid_is_3564_and_agent_is_null()
      throws Exception {
    ubiEvent.setPageId(2481888);
    ubiEvent.setAppId(3564);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2481888_and_appid_is_3564_and_agent_is_not_null1()
      throws Exception {
    ubiEvent.setPageId(2481888);
    ubiEvent.setAppId(3564);
    ClientData clientData = new ClientData();
    clientData.setAgent("ebayUserAgent/eBayAndroid");
    ubiEvent.setClientData(clientData);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2481888_and_appid_is_3564_and_agent_is_not_null2()
      throws Exception {
    ubiEvent.setPageId(2481888);
    ubiEvent.setAppId(3564);
    ClientData clientData = new ClientData();
    clientData.setAgent("ebay");
    ubiEvent.setClientData(clientData);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2053898_and_urlQueryPage_is_not_null1()
      throws Exception {
    ubiEvent.setPageId(2053898);
    ubiEvent.setUrlQueryString("page=MainCheckoutPage");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2053898_and_urlQueryPage_is_not_null2()
      throws Exception {
    ubiEvent.setPageId(2053898);
    ubiEvent.setUrlQueryString("page=ebay");
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(false, ubiEvent.isPartialValidPage());
  }

  @Test
  public void test_pageId_is_2053898_and_urlQueryPage_is_not_null3()
      throws Exception {
    ubiEvent.setPageId(2053898);
    ubiEvent.setUrlQueryString("page=MainCheckoutPage");
    ubiEvent.setApplicationPayload("page=ryprender");
    Map<String, String> sojA = new HashMap<>();
    sojA.put("page", "ryprender");
    rawEvent.setSojA(sojA);
    partialValidPageParser.parse(rawEvent, ubiEvent);
    Assertions.assertEquals(true, ubiEvent.isPartialValidPage());
  }
}
