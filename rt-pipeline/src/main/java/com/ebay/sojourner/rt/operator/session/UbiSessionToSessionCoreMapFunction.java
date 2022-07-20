package com.ebay.sojourner.rt.operator.session;

import com.ebay.sojourner.common.model.AgentHash;
import com.ebay.sojourner.common.model.Guid;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Base64Ebay;
import com.ebay.sojourner.common.util.BitUtils;
import com.ebay.sojourner.common.util.Constants;
import com.ebay.sojourner.common.util.GUID2Date;
import com.ebay.sojourner.common.util.IsValidGuid;
import com.ebay.sojourner.common.util.LkpManager;
import com.ebay.sojourner.common.util.RulePriorityUtils;
import com.ebay.sojourner.common.util.SessionCoreHelper;
import com.ebay.sojourner.common.util.SessionFlags;
import com.ebay.sojourner.common.util.SojTimestamp;
import com.ebay.sojourner.common.util.TypeTransformUtil;
import com.ebay.sojourner.common.util.UbiLookups;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

@Slf4j
public class UbiSessionToSessionCoreMapFunction extends RichMapFunction<UbiSession, SessionCore> {

  @Override
  public SessionCore map(UbiSession session) throws Exception {
    SessionCore core = new SessionCore();
    core.setAbsEventCnt(session.getAbsEventCnt());

    if (StringUtils.isNotBlank(session.getUserAgent())) {
      long[] long4AgentHash = TypeTransformUtil
          .md522Long(TypeTransformUtil.getMD5(session.getUserAgent()));
      AgentHash agentHash = new AgentHash();
      agentHash.setAgentHash1(long4AgentHash[0]);
      agentHash.setAgentHash2(long4AgentHash[1]);
      core.setUserAgent(agentHash);
    } else {
      AgentHash agentHash = new AgentHash();
      agentHash.setAgentHash1(0L);
      agentHash.setAgentHash2(0L);
      core.setUserAgent(agentHash);
    }

    core.setIp(TypeTransformUtil.ipToInt(session.getIp()) == null ? 0
        : TypeTransformUtil.ipToInt(session.getIp()));
    core.setBotFlag(RulePriorityUtils.getHighPriorityBotFlag(
        session.getBotFlagList()));
    if (session.getFirstCguid() != null) {
      long[] long4Cguid = TypeTransformUtil.md522Long(session.getFirstCguid());
      Guid cguid = new Guid();
      cguid.setGuid1(long4Cguid[0]);
      cguid.setGuid2(long4Cguid[1]);
      core.setCguid(cguid);
    } else {
      Guid cguid = new Guid();
      cguid.setGuid1(0L);
      cguid.setGuid2(0L);
      core.setCguid(cguid);
    }

    if (session.getGuid() != null) {
      long[] long4Cguid = TypeTransformUtil.md522Long(session.getGuid());
      Guid guid = new Guid();
      guid.setGuid1(long4Cguid[0]);
      guid.setGuid2(long4Cguid[1]);
      core.setGuid(guid);
    } else {
      Guid cguid = new Guid();
      cguid.setGuid1(0L);
      cguid.setGuid2(0L);
      core.setGuid(cguid);
    }

    core.setAppId(session.getFirstAppId());
    core.setFlags(getFlags(session));
    core.setValidPageCnt(session.getValidPageCnt());
    core.setSessionStartDt(session.getSessionStartDt());
    core.setAbsEndTimestamp(session.getAbsEndTimestamp());
    core.setAbsStartTimestamp(session.getAbsStartTimestamp());
    // handle IAB
    if (session.getNonIframeRdtEventCnt() > 0 && checkIabAgent(session.getUserAgent())) {
      core.setFlags(BitUtils.setBit(core.getFlags(), SessionFlags.IAB_AGENT));
    }

    if (BitUtils.isBitSet(core.getFlags(), SessionFlags.AGENT_STRING_DIFF)
        && StringUtils.isNotBlank(session.getAgentString())) {
      long[] long4AgentHash =
          TypeTransformUtil.md522Long(TypeTransformUtil.getMD5(session.getAgentString()));
      AgentHash agentHash = new AgentHash();
      agentHash.setAgentHash1(long4AgentHash[0]);
      agentHash.setAgentHash2(long4AgentHash[1]);
      core.setAgentString(agentHash);
    } else if (StringUtils.isBlank(session.getAgentString())) {
      AgentHash agentHash = new AgentHash();
      agentHash.setAgentHash1(0L);
      agentHash.setAgentHash2(0L);
      core.setAgentString(agentHash);
    }

    if (!StringUtils.equals(session.getIp(), session.getExInternalIp())) {
      core.setFlags(BitUtils.setBit(core.getFlags(), SessionFlags.EXINTERNALIP_NONTRIMED_DIFF));
      core.setExInternalIpNonTrim(TypeTransformUtil.ipToInt(session.getExInternalIp()));
    }

    String eiipTrimed = null;
    if (session.getExInternalIp() != null) {
      eiipTrimed = session.getExInternalIp().trim();
    }
    if (!StringUtils.equals(session.getIp(), eiipTrimed)) {
      core.setFlags(BitUtils.setBit(core.getFlags(), SessionFlags.EXINTERNALIP_DIFF));
      core.setExInternalIp(TypeTransformUtil.ipToInt(eiipTrimed));
    }

    // TODO to match the incorrect old logic , just for 'data quality'
    AgentHash agentString = SessionCoreHelper.getAgentString(core);
    if (agentString.getAgentHash1() != 0L && agentString.getAgentHash2() != 0L) {
      String agentBase64 = Base64Ebay.encode(session.getAgentString().getBytes());
      String agentStrAfterBase64 = Base64Ebay.decodeUTF8(agentBase64);

      long[] long4AgentHash = TypeTransformUtil
          .md522Long(TypeTransformUtil.getMD5(agentStrAfterBase64));
      AgentHash agentAfterBase64 = new AgentHash();
      agentAfterBase64.setAgentHash1(long4AgentHash[0]);
      agentAfterBase64.setAgentHash2(long4AgentHash[1]);

      if (!core.getUserAgent().equals(agentAfterBase64)) {
        core.setFlags(BitUtils.setBit(core.getFlags(),
            SessionFlags.AGENT_STRING_AFTER_BASE64_DIFF));
        core.setAgentStringAfterBase64(agentAfterBase64);
      }
    }

    return core;
  }

  private boolean checkIabAgent(String agent) {
    if (StringUtils.isNotBlank(agent)) {
      for (String iabAgentReg : LkpManager.getInstance().getIabAgentRegs()) {
        if (agent.toLowerCase().contains(iabAgentReg)) {
          return true;
        }
      }
    }
    return false;
  }

  private int getFlags(UbiSession session) throws Exception {
    final int DIRECT_SESSION_SRC = 1;
    final int SINGLE_PAGE_SESSION = 1;
    int flags = 0;
    if (session.getSingleClickSessionFlag() == null) {
      flags = BitUtils.setBit(flags, SessionFlags.SINGLE_CLICK_NULL_POS);
    }
    if (session.getSingleClickSessionFlag() != null && session.getSingleClickSessionFlag()) {
      flags = BitUtils.setBit(flags, SessionFlags.SINGLE_CLICK_FLAGS_POS);
    }
    if (session.getBidBinConfirmFlag() != null && session.getBidBinConfirmFlag()) {
      flags = BitUtils.setBit(flags, SessionFlags.BID_BIN_CONFIRM_FLAGS_POS);
    }
    if (session.getAgentCnt() > 1) {
      flags = BitUtils.setBit(flags, SessionFlags.AGENT_HOPER_FLAGS_POS);
    }
    if (isNewGuid(session.getGuid(), session.getStartTimestamp())) {
      flags = BitUtils.setBit(flags, SessionFlags.NEW_GUID_FLAGS_POS);
    }
    if (session.getValidPageCnt() == session.getHomepageCnt()) {
      flags = BitUtils.setBit(flags, SessionFlags.HOME_PAGE_FLAGS_POS);
    }
    if (session.getValidPageCnt() == session.getFamilyViCnt()) {
      flags = BitUtils.setBit(flags, SessionFlags.FAMILY_VI_FLAGS_POS);
    }
    if (session.getValidPageCnt() == session.getSigninPageCnt()) {
      flags = BitUtils.setBit(flags, SessionFlags.SIGN_IN_FLAGS_POS);
    }
    if (session.getFirstUserId() == null) {
      flags = BitUtils.setBit(flags, SessionFlags.NO_UID_FLAGS_POS);
    }
    if (session.getTrafficSrcId() == DIRECT_SESSION_SRC) {
      flags = BitUtils.setBit(flags, SessionFlags.DIRECT_FLAGS_POS);
    }
    if (UbiLookups.getInstance().getMktgTraficSrcIds().contains(session.getTrafficSrcId())) {
      flags = BitUtils.setBit(flags, SessionFlags.MKTG_FLAGS_POS);
    }
    if (UbiLookups.getInstance().getNonbrowserCobrands().contains(session.getCobrand())) {
      flags = BitUtils.setBit(flags, SessionFlags.SITE_FLAGS_POS);
    }

    if (StringUtils.isNotBlank(session.getAgentString())
        && UbiLookups.getInstance().getAgentMatcher().match(session.getAgentString())) {
      flags = BitUtils.setBit(flags, SessionFlags.DECLARATIVE_AGENT_FLAGS_POS);
    }

    if (session.getValidPageCnt() == SINGLE_PAGE_SESSION) {
      flags = BitUtils.setBit(flags, SessionFlags.SPS_SESSION_POS);
    }

    if (session.getNonIframeRdtEventCnt() == 0) {
      flags = BitUtils.setBit(flags, SessionFlags.ZERO_NON_IFRAME_RDT);
    }

    if (!StringUtils.equals(session.getUserAgent(), session.getAgentString())) {
      flags = BitUtils.setBit(flags, SessionFlags.AGENT_STRING_DIFF);
    }

    if (session.getIsIPExternal()!=null && session.getIsIPExternal().booleanValue()) {
      flags = BitUtils.setBit(flags, SessionFlags.IS_IP_EXTERNAL_FLAG_POS);
    }

    if (session.getStartTimestamp() != null) {
      flags = BitUtils.setBit(flags, SessionFlags.VALID_SESSSION_FLAG_POS);
    }


    if (IsValidGuid.isValidGuid(session.getGuid())) {
      flags = BitUtils.setBit(flags, SessionFlags.VALID_GUID_FLAG_POS);
    }

    return flags;
  }

  private boolean isNewGuid(String guid, Long startTimestamp) {
    try {
      if (startTimestamp != null) {
        long guidTimestamp = GUID2Date.getTimestamp(guid);
        long startTimestampInUnix = SojTimestamp.getUnixTimestamp(startTimestamp);
        long minTimestamp = startTimestampInUnix - Constants.MINUS_GUID_MIN_MS;
        long maxTimestamp = startTimestampInUnix + Constants.PLUS_GUID_MAX_MS;
        if (guidTimestamp >= minTimestamp && guidTimestamp <= maxTimestamp) {
          return true;
        }
      }
    } catch (RuntimeException e) {
      return false;
    }
    return false;
  }

}
