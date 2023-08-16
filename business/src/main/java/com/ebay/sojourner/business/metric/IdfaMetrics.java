package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.SOJNVL;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;

/**
 * To extract valid idfa tag from event application_payload for session
 * @author donlu
 */
public class IdfaMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  @Override
  public void init() throws Exception {
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {

  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {

    UbiSession ubiSession = sessionAccumulator.getUbiSession();

    List<Integer> VALID_PAGE_IDS = Arrays.asList(
        2356359,2367320,2054121,2050465,2047936,2062300,2051457,2054060,2065434,2054081,2065432,
        2051248,2051249,2050535,2057087,2054081,2059087,2056372,2052310,2054060, 2053277,2058946,
        2054180,2050494,2050495,2058483,2050605,2050606,1673581,1698105,2034596,2041594,1677709
    );
    String app = SOJNVL.getTagValue(event.getApplicationPayload(), "app");
    String idfa = SOJNVL.getTagValue(event.getApplicationPayload(), "idfa");
    if (VALID_PAGE_IDS.contains(event.getPageId())
      && StringUtils.isNotEmpty(app) && StringUtils.isNumeric(app)
      && event.getSiteId() != -1
      && StringUtils.isEmpty(ubiSession.getIdfa())
      && StringUtils.isNotEmpty(idfa)
    ) {
      ubiSession.setIdfa(idfa.substring(0, 36));
    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }
}
