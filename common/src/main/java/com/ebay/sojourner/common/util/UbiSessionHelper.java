package com.ebay.sojourner.common.util;

import com.ebay.sojourner.common.model.AgentAttribute;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.model.UbiSession;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Xiaoding
 */
public class UbiSessionHelper {

  public static final float DEFAULT_LOAD_FACTOR = .75F;
  public static final int IAB_MAX_CAPACITY =
      100 * 1024; // 250 * 1024 * 1024 = 250m - refer io.sort.mb (default spill size)
  public static final int IAB_INITIAL_CAPACITY = 10 * 1024; // 16 * 1024 * 1024 = 16m

  private static Map<String, Boolean> iabCache =
      new ConcurrentHashMap<>(IAB_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);

  private List<String> iabAgentRegs;

  public UbiSessionHelper() {
    iabAgentRegs = LkpManager.getInstance().getIabAgentRegs();
  }

  public static boolean isAgentDeclarative(AgentAttribute agentAttribute)
      throws IOException, InterruptedException {
    String userAgent =
        String.valueOf(
            TypeTransformUtil.recoveryMD5(agentAttribute.getAgent().getAgentHash1().longValue(),
                agentAttribute.getAgent().getAgentHash2().longValue()));
    return StringUtils.isNotBlank(userAgent)
        && UbiLookups.getInstance().getAgentMatcher().match(userAgent);
  }

  public static boolean isAgentDeclarative(String userAgent)
      throws IOException, InterruptedException {
    return StringUtils.isNotBlank(userAgent)
        && UbiLookups.getInstance().getAgentMatcher().match(userAgent);
  }

  public static boolean isNonIframRdtCountZero(Object session) {

    if (session instanceof SessionCore) {
      SessionCore intermediateSession = (SessionCore) session;
      return SessionCoreHelper.isNonIframRdtCountZero(intermediateSession);
    } else {
      UbiSession ubiSession = (UbiSession) session;
      return ubiSession.getNonIframeRdtEventCnt() == 0;
    }

  }

  public boolean isIabAgent(UbiSession session) throws InterruptedException {
    if (session.getNonIframeRdtEventCnt() > 0 && session.getUserAgent() != null) {
      String userAgent = session.getUserAgent();
      Boolean whether = iabCache.get(userAgent);
      if (whether == null) {
        whether = checkIabAgent(userAgent);
        if (iabCache.size() < IAB_MAX_CAPACITY) {
          iabCache.put(userAgent, whether);
        }
      }
      return whether;
    }

    return false;
  }

  protected boolean checkIabAgent(String agent) {
    iabAgentRegs = LkpManager.getInstance().getIabAgentRegs();
    if (StringUtils.isNotBlank(agent)) {
      for (String iabAgentReg : iabAgentRegs) {
        if (agent.toLowerCase().contains(iabAgentReg)) {
          return true;
        }
      }
    }
    return false;
  }

}
