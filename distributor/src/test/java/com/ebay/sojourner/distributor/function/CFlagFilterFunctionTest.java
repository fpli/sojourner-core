package com.ebay.sojourner.distributor.function;


import static com.ebay.sojourner.common.constant.ApplicationPayloadTags.CFLGS_TAG;

import com.ebay.sojourner.common.model.SojEvent;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CFlagFilterFunctionTest {

  CFlagFilterFunction cFlagFilterFunction;
  SojEvent sojEvent;

  @BeforeEach
  void setUp() {
    cFlagFilterFunction = new CFlagFilterFunction();
    sojEvent = new SojEvent();
    sojEvent.setPageId(3084);
  }

  @Test
  void testFilter_no_cflag() throws Exception {
    Map<String, String> appPayload = new HashMap<>();
    appPayload.put("a", "b");

    sojEvent.setApplicationPayload(appPayload);
    boolean result = cFlagFilterFunction.filter(sojEvent);
    Assertions.assertThat(result).isTrue();
  }

  @Test
  void testFilter_cflag_hit_bit() throws Exception {
    Map<String, String> appPayload = new HashMap<>();
    appPayload.put(CFLGS_TAG, "QA**");
    sojEvent.setApplicationPayload(appPayload);
    boolean result = cFlagFilterFunction.filter(sojEvent);
    Assertions.assertThat(result).isTrue();
  }

  @Test
  void testFilter_cflag_not_hit_bit() throws Exception {
    Map<String, String> appPayload = new HashMap<>();
    appPayload.put(CFLGS_TAG, "wA**");

    sojEvent.setApplicationPayload(appPayload);
    boolean result = cFlagFilterFunction.filter(sojEvent);
    Assertions.assertThat(result).isFalse();
  }

}