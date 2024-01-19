package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.SOJNVL;
import com.ebay.sojourner.common.util.SojEventTimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;


/**
 * To extract buyer_id from event application_payload for session
 * @author donlu
 */
public class BuyerIdMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {
  private static final String BUYER_TAG = "buyer_id";
  private static final int BUYER_ID_MAX_LEN = 18;

  @Override
  public void init() throws Exception {
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {
    String buyerId = SOJNVL.getTagValue(event.getApplicationPayload(), BUYER_TAG);
    boolean isEarlyEvent = SojEventTimeUtil.isEarlyEvent(
            event.getEventTimestamp(), sessionAccumulator.getUbiSession().getAbsStartTimestamp());
    if ((isEarlyEvent || StringUtils.isEmpty(sessionAccumulator.getUbiSession().getBuyerId()))
            && StringUtils.isNotEmpty(buyerId)
            && buyerId.length() <= BUYER_ID_MAX_LEN
            && NumberUtils.isDigits(buyerId)) {
      sessionAccumulator.getUbiSession().setBuyerId(buyerId);
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }
}
