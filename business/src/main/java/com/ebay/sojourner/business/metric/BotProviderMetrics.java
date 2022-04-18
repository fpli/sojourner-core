package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SOJNVL;
import com.ebay.sojourner.common.util.SOJURLDecodeEscape;

public class BotProviderMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  @Override
  public void init() throws Exception {

  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {

  }

  @Override
  public void feed(UbiEvent ubiEvent, SessionAccumulator sessionAccumulator) throws Exception {

    String applicationPayload = ubiEvent.getApplicationPayload();
    String botProvider = SOJURLDecodeEscape
        .decodeEscapes(SOJNVL.getTagValue(applicationPayload, "bot_provider"), '%');

    sessionAccumulator.getUbiSession().setBotProvider(botProvider);
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {

  }
}
