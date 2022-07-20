package com.ebay.sojourner.common.util;


import com.ebay.sojourner.common.model.AgentHash;
import com.ebay.sojourner.common.model.SessionCore;

/**
 * @author weifang
 */
public abstract class SessionCoreHelper {

  public static boolean isSingleClickSession(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.SINGLE_CLICK_FLAGS_POS);
    }
    return false;
  }

  public static boolean isSingleClickNull(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.SINGLE_CLICK_NULL_POS);
    }
    return false;
  }


  public static boolean isBidBinConfirm(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.BID_BIN_CONFIRM_FLAGS_POS);
    }
    return false;
  }

  public static boolean isAgentHoper(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.AGENT_HOPER_FLAGS_POS);
    }
    return false;
  }

  public static boolean isNewGuid(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.NEW_GUID_FLAGS_POS);
    }
    return false;
  }

  public static boolean isHomePage(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.HOME_PAGE_FLAGS_POS);
    }
    return false;
  }

  public static boolean isFamilyVi(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.FAMILY_VI_FLAGS_POS);
    }
    return false;
  }

  public static boolean isSignIn(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.SIGN_IN_FLAGS_POS);
    }
    return false;
  }

  public static boolean isNoUid(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.NO_UID_FLAGS_POS);
    }
    return false;
  }

  public static boolean isSps(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.SPS_SESSION_POS);
    }

    return false;
  }

  public static boolean isDirect(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.DIRECT_FLAGS_POS);
    }
    return false;
  }

  public static boolean isMktg(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.MKTG_FLAGS_POS);
    }
    return false;
  }

  public static boolean isSite(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.SITE_FLAGS_POS);
    }
    return false;
  }

  public static boolean isAgentDeclarative(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.DECLARATIVE_AGENT_FLAGS_POS);
    }
    return false;
  }

  public static boolean isNonIframRdtCountZero(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.ZERO_NON_IFRAME_RDT);
    }
    return false;
  }

  public static boolean isAgentStringDiff(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.AGENT_STRING_DIFF);
    }
    return false;
  }

  public static AgentHash getAgentString(SessionCore session) {
    if (isAgentStringDiff(session)) {
      return session.getAgentString();
    } else {
      return session.getUserAgent();
    }
  }

  public static boolean isExInternalIpDiff(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.EXINTERNALIP_DIFF);
    }
    return false;
  }

  public static boolean isExInternalIpNonTrimDiff(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.EXINTERNALIP_NONTRIMED_DIFF);
    }
    return false;
  }

  public static boolean isAgentStringAfterBase64Diff(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.AGENT_STRING_AFTER_BASE64_DIFF);
    }
    return false;
  }

  public static Integer getExInternalIp(SessionCore session) {
    if (isExInternalIpDiff(session)) {
      return session.getExInternalIp();
    } else {
      return session.getIp();
    }
  }

  public static Integer getExInternalIpNonTrim(SessionCore session) {
    if (isExInternalIpNonTrimDiff(session)) {
      return session.getExInternalIpNonTrim();
    } else {
      return session.getIp();
    }
  }

  public static AgentHash getAgentStringAfterBase64(SessionCore session) {
    if (isAgentStringAfterBase64Diff(session)) {
      return session.getAgentStringAfterBase64();
    } else {
      return session.getUserAgent();
    }
  }

  public static boolean isIabAgent(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.IAB_AGENT);
    }
    return false;
  }

  public static boolean isYesterdaySession(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.YESTERDAY_SESSION);
    }
    return false;
  }

  public static boolean isIPExternal(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.IS_IP_EXTERNAL_FLAG_POS);
    }
    return false;
  }

  public static boolean isValidSession(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.VALID_SESSSION_FLAG_POS);
    }
    return false;
  }

  public static boolean isValidGuid(SessionCore session) {
    if (session.getFlags() != null) {
      return BitUtils.isBitSet(session.getFlags(), SessionFlags.VALID_GUID_FLAG_POS);
    }
    return false;
  }
}
