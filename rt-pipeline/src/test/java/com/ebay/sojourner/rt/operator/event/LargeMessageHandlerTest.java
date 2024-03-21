package com.ebay.sojourner.rt.operator.event;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class LargeMessageHandlerTest {

  @Test
  public void testUrlQueryString1() {

    LargeMessageHandler largeMessageFilterFunction = new LargeMessageHandler(102400, 100, true, ImmutableSet.of());
    String urlQueryString = "/V4Ajax?reqttype=JSON&clientType=Firefox%3A89%3A&v=0&resptype=JSON&pId=6115&svcid=AC_DETECTION_SERVICE&stok=-1877446575";
    String expectedString = "/V4Ajax?reqttype=JSON&clientType=Firefox%3A89%3A&v=0&resptype=JSON&pId=6115";
    String actualString = largeMessageFilterFunction.truncateUrlQueryString(urlQueryString);
    Assertions.assertEquals(expectedString, actualString);
  }

  @Test
  public void testUrlQueryString2() {

    LargeMessageHandler largeMessageFilterFunction = new LargeMessageHandler(102400, 10, true, ImmutableSet.of());
    String urlQueryString = "/V4Ajax?reqttype=JSON&clientType=Firefox%3A89%3A&v=0&resptype=JSON&pId=6115&svcid=AC_DETECTION_SERVICE&stok=-1877446575";
    String expectedString = "";
    String actualString = largeMessageFilterFunction.truncateUrlQueryString(urlQueryString);
    Assertions.assertEquals(expectedString, actualString);
  }

  @Test
  public void testUrlQueryString3() {

    LargeMessageHandler largeMessageFilterFunction = new LargeMessageHandler(102400, 1, true, ImmutableSet.of());
    String urlQueryString = "&/V4Ajax";
    String expectedString = "";
    String actualString = largeMessageFilterFunction.truncateUrlQueryString(urlQueryString);
    Assertions.assertEquals(expectedString, actualString);
  }
}
