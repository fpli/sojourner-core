package com.ebay.sojourner.common.util;

import com.ebay.sojourner.common.model.ClientData;
import com.ebay.sojourner.common.model.UbiEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EventHashTest {

  private UbiEvent ubiEvent;
  private EventHash eventHash;
  private ClientData clientData;

  @BeforeEach
  public void setup() {
    ubiEvent = new UbiEvent();
    eventHash = new EventHash();
    clientData = new ClientData();
  }

  @Test
  public void test_ubievent_hashcode() {
    clientData.setAgent("agent");
    ubiEvent.setCookies("cookies");
    ubiEvent.setFlags("flags");
    ubiEvent.setItemId(1L);
    ubiEvent.setPageName("pagename");
    ubiEvent.setRdt(true);
    ubiEvent.setRefererHash(1L);
    ubiEvent.setReferrer("referrer");
    ubiEvent.setRegu(1);
    ubiEvent.setSessionStartDt(1L);
    ubiEvent.setSiteId(2);
    ubiEvent.setSqr("sqr");
    ubiEvent.setStaticPageType(1);
    ubiEvent.setUrlQueryString("urlQueryString");
    ubiEvent.setUserId("userid");
    ubiEvent.setWebServer("webserver");
    ubiEvent.setCurrentImprId(1L);
    ubiEvent.setSourceImprId(1L);
    ubiEvent.setClientData(clientData);
    Assertions.assertEquals(-792913298, eventHash.hashCode(ubiEvent));
  }
}
