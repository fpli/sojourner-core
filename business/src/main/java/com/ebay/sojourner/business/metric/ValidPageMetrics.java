package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.model.UbiSession;
import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.common.util.PropertyUtils;
import com.ebay.sojourner.common.util.SojEventTimeUtil;
import com.ebay.sojourner.common.util.UBIConfig;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * SUM ( CASE WHEN e.partial_valid_page = 0 THEN 0 WHEN PG.FRAME_BASED_PAGE_YN_ID = 1 THEN 0 WHEN
 * e.page_id IN ( 4600 , 0 , 4561 , 4394 , 4105 , 3936 , 4370 , 4369 , 4447 , 3848 , 3847 , 3846 ,
 * 3849 , 4648 , 3872 , 4626 , 2219 , 4490 , 4016 , 4813 , 4684 , 4433 , 4803 , 4827 , 4843 , 4909 ,
 * 3054 , 4095 , 5024 , 3880 , 4887 , 4818 , 4599 , 2608 , 5277 , 5209 , 5494 , 3856 , 5457 , 5476 ,
 * 5609 , 3676 , 4346 , 4855 , 1992 , 4931 , 5074 , 4993 , 4223 , 4592 ) THEN 0 WHEN e.page_id IN (
 * 2720 , 1892 , 1893 , 4008 , 3288 , 2015 ,4699 , 4859 ) THEN 0 WHEN e.page_id IS NULL AND
 * e.cs_tracking = 1 THEN 0 ELSE 1 END ) AS valid_page_count ,
 *
 * @author kofeng
 */
public class ValidPageMetrics implements FieldMetrics<UbiEvent, SessionAccumulator> {

  private Set<Integer> invalidPageIds;

  @Override
  public void init() throws Exception {
    invalidPageIds =
        PropertyUtils.getIntegerSet(
            UBIConfig.getString(Property.INVALID_PAGE_IDS), Property.PROPERTY_DELIMITER);
  }

  @Override
  public void start(SessionAccumulator sessionAccumulator) throws Exception {
    sessionAccumulator.getUbiSession().setValidPageCnt(0);
  }

  @Override
  public void feed(UbiEvent event, SessionAccumulator sessionAccumulator) throws Exception {

    UbiSession ubiSession = sessionAccumulator.getUbiSession();
    // here simplify e.page_id IS NULL AND e.cs_tracking = 1 to e.page_id IS NULL
    // change logic to allign with caleb on 2018-02-26
    int csTracking = 0;
    if (StringUtils.isNotBlank(event.getUrlQueryString())
        && (event.getUrlQueryString().startsWith("/roverimp")
        || event.getUrlQueryString().contains("SojPageView"))) {
      csTracking = 1;
    }
    if (event.isPartialValidPage()
        && !event.isIframe()
        && ((event.getPageId() != -1 && !invalidPageIds.contains(event.getPageId()))
        || csTracking == 0)) {
      ubiSession
          .setValidPageCnt(ubiSession.getValidPageCnt() + 1);

      if (SojEventTimeUtil
          .isEarlyEvent(event.getEventTimestamp(), ubiSession.getStartTimestampForValidPage())) {
        ubiSession.setStartTimestampForValidPage(event.getEventTimestamp());
        ubiSession.setFirstAgent(event.getAgentInfo());
      }

      if (SojEventTimeUtil
          .isLateEvent(event.getEventTimestamp(), ubiSession.getEndTimestampForValidPage())) {
        ubiSession.setEndTimestampForValidPage(event.getEventTimestamp());
        ubiSession.setLastAgent(event.getAgentInfo());
      }

    }
  }

  @Override
  public void end(SessionAccumulator sessionAccumulator) throws Exception {
  }
}
