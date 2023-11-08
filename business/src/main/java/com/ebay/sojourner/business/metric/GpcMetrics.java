package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SOJNVL;
import org.apache.commons.lang3.StringUtils;

public class GpcMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  @Override
  public void init() throws Exception {

  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {

  }

  @Override
  public void feed(UbiEvent ubiEvent, SessionAccumulator sessionAccumulator) throws Exception {
    String gpc = SOJNVL.getTagValue(ubiEvent.getApplicationPayload(), "gpc");
    if (StringUtils.isNotBlank(gpc) && gpc.equals("1")) {
      sessionAccumulator.getUbiSession().setGpc(1);
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {

  }
}
