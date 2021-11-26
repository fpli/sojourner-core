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
    String result6 = SOJNVL.getTagValue("screenScale=2.0&tz=-5&formFactor=tablet&windowHeight=834&dm=Apple&inputCorrelator=0&dn=iPad&tzname=America%2FNew_York&uc=1&mos=iOS&osv=14.8.1&deviceAdvertisingOptOut=false&ul=en-US&theme=light&mtsts=2021-11-22T13%3A53%3A53.184189Z&windowWidth=1112&pagename=MotorsApp_ViewItem&app=35023&res=1112X834&efam=ITM&itm=255236620721&mav=2.9.0&g=3524382d17b197d726e35480015c3121&idfa=00000000-0000-0000-0000-000000000000&h=2d&nativeApp=true&p=3749770&mnt=wifi&t=100&eactn=CLIENT_PAGE_VIEW&gitCommitId=eba146301a6cba57ed023e02c44012fc5a36013d&ts=2021-11-22T13%3A53%3A53.184189Z&rq=109b88b823c7f199", "mtsts");
    assertThat(result1).isEqualTo("456");
    assertThat(result2).isEqualTo("456");
    assertThat(result3).isEqualTo("456");
    assertThat(result4).isEqualTo("45==6");
    assertThat(result5).isEqualTo("456");
    assertThat(result6).isEqualTo("2021-11-22T13%3A53%3A53.184189Z");
  }
}
