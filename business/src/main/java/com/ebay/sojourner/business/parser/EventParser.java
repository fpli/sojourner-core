package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;

public class EventParser extends RecordParser<RawEvent, UbiEvent> {

  public EventParser() throws Exception {
    initFieldParsers();
    init();
  }

  @Override
  public void initFieldParsers() {
    if (this.fieldParsers.isEmpty()) {
      // Keep insert order to reuse existed field normalization result
      addFieldParser(new IdentityParser());
      // icf
      addFieldParser(new IcfParser());
      addFieldParser(new TimestampParser());
      addFieldParser(new CiidParser());
      addFieldParser(new ClickIdParser());
      addFieldParser(new CookiesParser());
      addFieldParser(new FlagsParser());
      addFieldParser(new ItemIdParser());
      addFieldParser(new PageIdParser());
      addFieldParser(new RdtParser());
      addFieldParser(new RefererParser());
      addFieldParser(new ReferrerHashParser());
      addFieldParser(new ReguParser());
      addFieldParser(new ServerParser());
      addFieldParser(new SiidParser());
      addFieldParser(new SiteIdParser());
      addFieldParser(new SqrParser());
      addFieldParser(new UserIdParser());
      addFieldParser(new AgentInfoParser());
      addFieldParser(new ClientIPParser());
      addFieldParser(new IFrameParser());
      // Finding Flag should after Page Id
      addFieldParser(new FindingFlagParser());
      addFieldParser(new StaticPageTypeParser());
      // add appid for iphone data filter
      addFieldParser(new AppIdParser());
      // new metrics
      addFieldParser(new CobrandParser());
      addFieldParser(new PartialValidPageParser());
      // Jetstream columns
      addFieldParser(new JSColumnParser());
      addFieldParser(new HashCodeParser());
      addFieldParser(new RvParser());
    }
  }
}
