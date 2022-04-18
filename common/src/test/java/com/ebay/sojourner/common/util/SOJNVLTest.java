package com.ebay.sojourner.common.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class SOJNVLTest {

  @Test
  void test_getTagValue() {
    String result1 = SOJNVL.getTagValue("abc=123&dd=456", "dd");
    String result2 = SOJNVL.getTagValue("abc=123&_dd=456", "dd");
    String result3 = SOJNVL.getTagValue("abc=123&!dd=456", "dd");
    String result4 = SOJNVL.getTagValue("abc=123&!dd=45==6", "dd");
    String result5 = SOJNVL.getTagValue("abc=123&!dd=456&_EE=789", "dd");
    assertThat(result1).isEqualTo("456");
    assertThat(result2).isEqualTo("456");
    assertThat(result3).isEqualTo("456");
    assertThat(result4).isEqualTo("45==6");
    assertThat(result5).isEqualTo("456");
  }
}
