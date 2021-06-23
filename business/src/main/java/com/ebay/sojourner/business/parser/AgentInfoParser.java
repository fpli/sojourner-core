package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import org.apache.commons.lang3.StringUtils;

public class AgentInfoParser implements FieldParser<RawEvent, UbiEvent> {

  @Override
  public void init() throws Exception {
  }

  @Override
  public void parse(RawEvent rawEvent, UbiEvent ubiEvent) throws Exception {
    String agentInfo = rawEvent.getClientData().getAgent();
    if(StringUtils.isBlank(agentInfo) || agentInfo.equalsIgnoreCase("null")){
      ubiEvent.setAgentInfo("null");
    }else {
      ubiEvent.setAgentInfo(agentInfo);
    }
  }
}
