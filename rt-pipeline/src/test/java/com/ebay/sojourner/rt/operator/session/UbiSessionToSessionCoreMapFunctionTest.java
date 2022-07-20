package com.ebay.sojourner.rt.operator.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.model.UbiSession;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UbiSessionToSessionCoreMapFunctionTest {

  UbiSessionToSessionCoreMapFunction mapFunction;
  UbiSession ubiSession;

  @BeforeEach
  void setUp() {
    mapFunction = new UbiSessionToSessionCoreMapFunction();
    ubiSession = new UbiSession();
  }

  @Test
  void map_direct() throws Exception {
    ubiSession.setAbsEventCnt(3);
    Set<Integer> flagList = new HashSet<>();
    flagList.add(1);
    ubiSession.setBotFlagList(flagList);
    ubiSession.setFirstAppId(123);
    ubiSession.setValidPageCnt(1);
    ubiSession.setSessionStartDt(1579505089L);
    ubiSession.setAbsEndTimestamp(1579505089L);
    ubiSession.setAbsStartTimestamp(1579505089L);

    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getAbsEventCnt()).isEqualTo(3);
    assertThat(sessionCore.getBotFlag()).isEqualTo(1);
    assertThat(sessionCore.getAppId()).isEqualTo(123);
    assertThat(sessionCore.getValidPageCnt()).isEqualTo(1);
    assertThat(sessionCore.getSessionStartDt()).isEqualTo(1579505089L);
    assertThat(sessionCore.getAbsEndTimestamp()).isEqualTo(1579505089L);
    assertThat(sessionCore.getAbsStartTimestamp()).isEqualTo(1579505089L);
  }

  @Test
  void map_userAgent_isBlank() throws Exception {
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getUserAgent().getAgentHash1()).isEqualTo(0L);
    assertThat(sessionCore.getUserAgent().getAgentHash2()).isEqualTo(0L);
  }

  @Test
  void map_userAgent_isNotBlank() throws Exception {
    ubiSession.setUserAgent("test");
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getUserAgent().getAgentHash1()).isEqualTo(3980357870074394768L);
    assertThat(sessionCore.getUserAgent().getAgentHash2()).isEqualTo(8019629327650188716L);
  }

  @Test
  void map_ip() throws Exception {
    ubiSession.setIp("10.0.0.1");
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getIp()).isEqualTo(167772161);
  }

  @Test
  void map_firstCguid_isNotNull() throws Exception {
    ubiSession.setFirstCguid("123");
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getCguid().getGuid1()).isEqualTo(801);
    assertThat(sessionCore.getCguid().getGuid2()).isEqualTo(0);
  }

  @Test
  void map_firstCguid_isNull() throws Exception {
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getCguid().getGuid1()).isEqualTo(0);
    assertThat(sessionCore.getCguid().getGuid2()).isEqualTo(0);
  }

  @Test
  void map_guid_isNotNull() throws Exception {
    ubiSession.setGuid("123456");
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getGuid().getGuid1()).isEqualTo(6636321);
    assertThat(sessionCore.getGuid().getGuid2()).isEqualTo(0);
  }

  @Test
  void map_guid_isNull() throws Exception {
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getGuid().getGuid1()).isEqualTo(0);
    assertThat(sessionCore.getGuid().getGuid2()).isEqualTo(0);
  }

  @Test
  void map_agentString_isNotBlank() throws Exception {
    ubiSession.setAgentString("chrome");
    ubiSession.setUserAgent("safari");
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getAgentString().getAgentHash1()).isEqualTo(-4162274571123981227L);
    assertThat(sessionCore.getAgentString().getAgentHash2()).isEqualTo(-4384696659385354341L);
  }

  @Test
  void map_agentString_isBlank() throws Exception {
    ubiSession.setAgentString("");
    ubiSession.setUserAgent("safari");
    SessionCore sessionCore = mapFunction.map(ubiSession);

    assertThat(sessionCore.getAgentString().getAgentHash1()).isEqualTo(0);
    assertThat(sessionCore.getAgentString().getAgentHash2()).isEqualTo(0);
  }
}