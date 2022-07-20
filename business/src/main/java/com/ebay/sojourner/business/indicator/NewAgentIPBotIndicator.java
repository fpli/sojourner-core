package com.ebay.sojourner.business.indicator;

import com.ebay.sojourner.common.model.AgentIpAttribute;
import com.ebay.sojourner.common.model.AgentIpAttributeAccumulator;
import com.ebay.sojourner.common.model.SessionCore;
import com.ebay.sojourner.common.util.BotFilter;
import com.ebay.sojourner.common.util.SessionCoreHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NewAgentIPBotIndicator<Source> extends
    AbstractIndicator<Source, AgentIpAttributeAccumulator> {

  private int botFlag;

  public NewAgentIPBotIndicator(BotFilter botFilter, int botFlag) {
    this.botFilter = botFilter;
    this.botFlag = botFlag;
  }

  @Override
  public void start(AgentIpAttributeAccumulator agentIpAttributeAccumulator) throws Exception {
    agentIpAttributeAccumulator.getAgentIpAttribute().clear();
  }

  @Override
  public void feed(Source source,
      AgentIpAttributeAccumulator agentIpAttributeAccumulator) throws Exception {
    if (source instanceof SessionCore) {
      SessionCore sessionCore = (SessionCore) source;
      if (isValid(sessionCore)) {
        agentIpAttributeAccumulator.getAgentIpAttribute().feed(sessionCore, botFlag);
      }
    } else {
      AgentIpAttribute agentIpAttribute = (AgentIpAttribute) source;
      agentIpAttributeAccumulator
          .getAgentIpAttribute()
          .merge(agentIpAttribute, botFlag);
    }
  }

  @Override
  public boolean filter(Source source,
      AgentIpAttributeAccumulator agentIpAttributeAccumulator) throws Exception {
    if (source instanceof SessionCore) {
      SessionCore sessionCore = (SessionCore) source;
      if (sessionCore.getBotFlag() != null && sessionCore.getBotFlag() > 0
          && sessionCore.getBotFlag() < 200) {
        return true;
      }
    }
    return false;
  }

  private boolean isValid(SessionCore sessionCore) {
    return SessionCoreHelper.isIPExternal(sessionCore)
        && SessionCoreHelper.isValidSession(sessionCore)
        && SessionCoreHelper.isValidGuid(sessionCore);
  }
}
