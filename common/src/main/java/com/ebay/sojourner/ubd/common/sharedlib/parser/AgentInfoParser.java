package com.ebay.sojourner.ubd.common.sharedlib.parser;

import com.ebay.sojourner.ubd.common.model.RawEvent;
import com.ebay.sojourner.ubd.common.model.UbiEvent;
import org.apache.commons.lang3.StringUtils;

public class AgentInfoParser implements FieldParser<RawEvent, UbiEvent> {

  @Override
  public void init() throws Exception {
  }

  @Override
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {
    String agentInfo = rawEvent.getClientData().getAgent();
    if (StringUtils.isNotBlank(agentInfo)) {
      ubiEvent.setAgentInfo(agentInfo);
    }
  }
}
