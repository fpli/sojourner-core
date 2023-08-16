package com.ebay.sojourner.business.metric;

import com.ebay.sojourner.common.model.SessionAccumulator;
import com.ebay.sojourner.common.model.UbiEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionMetrics extends RecordMetrics<UbiEvent, SessionAccumulator> {

  private static volatile SessionMetrics sessionMetrics;

  private SessionMetrics() {
    initFieldMetrics();
    try {
      init();
    } catch (Exception e) {
      log.error("Failed to init session metrics", e);
    }
  }

  public static SessionMetrics getInstance() {
    if (sessionMetrics == null) {
      synchronized (SessionMetrics.class) {
        if (sessionMetrics == null) {
          sessionMetrics = new SessionMetrics();
        }
      }
    }
    return sessionMetrics;
  }

  @Override
  public void initFieldMetrics() {

    addFieldMetrics(new SingleClickFlagMetrics());
    addFieldMetrics(new AgentIPMetrics());
    addFieldMetrics(new AgentStringMetrics());
    addFieldMetrics(new SessionStartDtMetrics());
    addFieldMetrics(new SessionDwellMetrics());

    // Keep insert order to reuse existed field end metrics
    addFieldMetrics(new ReferrerMetrics());
    addFieldMetrics(new FindingFlagMetrics());
    addFieldMetrics(new SiteFlagMetrics());
    addFieldMetrics(new AttributeFlagMetrics());
    addFieldMetrics(new BidCntMetrics());
    addFieldMetrics(new BinCntMetrics());
    addFieldMetrics(new SiidCntMetrics());

    // Set abEventCnt and eventCnt both
    addFieldMetrics(new EventCntMetrics());
    addFieldMetrics(new OldSessionSkeyMetrics());

    //        addFieldMetrics(new SessionStartDtMetrics());
    //        addFieldMetrics(new TimestampMetrics());
    addFieldMetrics(new UserIdMetrics());
    addFieldMetrics(new ViCoreMetrics());
    addFieldMetrics(new WatchCntMetric());
    //        addFieldMetrics(new AgentIPMetrics());
    // add for iphone data filter
    addFieldMetrics(new AppIdMetrics());
    //        addFieldMetrics(new SingleClickFlagMetrics());
    addFieldMetrics(new BidBinConfirmFlagMetrics());
    addFieldMetrics(new BotFlagsMetrics());
    // few more new metrics
    addFieldMetrics(new SiteIdMetrics());
    addFieldMetrics(new CobrandMetrics());
    addFieldMetrics(new CguidMetrics());
    addFieldMetrics(new GrCntMetrics());
    addFieldMetrics(new Gr1CntMetrics());
    addFieldMetrics(new MyebayCntMetrics());
    addFieldMetrics(new LogdnCntMetrics());
    addFieldMetrics(new HomepgCntMetrics());
    addFieldMetrics(new FirstMappedUserIdMetrics());

    //jetstream sesssion metrics
    addFieldMetrics(new ServEventCntMetrics());
    addFieldMetrics(new AsqCntMetrics());
    addFieldMetrics(new AtcCntMetrics());
    addFieldMetrics(new AtlCntMetrics());
    addFieldMetrics(new BoCntMetrics());
    addFieldMetrics(new SrpCntMetrics());
    addFieldMetrics(new PageIdMetrics());
    addFieldMetrics(new AddressMetrics());
    addFieldMetrics(new LineSpeedMetrics());
    // move traffic source id to bottom
    addFieldMetrics(new TrafficSourceIdMetrics());
    // Add extra metrics for new bots
    addFieldMetrics(new LndgPageIdMetrics());
    addFieldMetrics(new ValidPageMetrics());
    //        addFieldMetrics(new AgentStringMetrics());
    addFieldMetrics(new FmlyViCntMetrics());
    addFieldMetrics(new SearchCntMetrics());
    addFieldMetrics(new PageCntMetrics());
    addFieldMetrics(new MaxScsSeqNumMetrics());
    addFieldMetrics(new MultiColsMetrics());
    addFieldMetrics(new TimestampMetrics());
    addFieldMetrics(new IsRVMetrics());

    // metrics for bot provider
    addFieldMetrics(new BotProviderMetrics());


    // Put bot flag
    //        addFieldMetrics(new BotFlagMetrics());

    // Metrics used by iOS_HP_bot rule detection
    addFieldMetrics(new FirstIosHpMetrics());
    addFieldMetrics(new GroundEventMetrics());
    addFieldMetrics(new IdfaMetrics());
    addFieldMetrics(new ValidPageIosMetrics());

  }
}
