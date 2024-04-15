package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.business.parser.cjs.CjsParser;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;

import static com.ebay.sojourner.common.constant.ConfigProperty.CJS_PARSER_DISABLED;


public class EventParser extends RecordParser<RawEvent, UbiEvent> {

    public EventParser(ParserContext context) throws Exception {
        initFieldParsers(context);
        init(context);
    }

    @Override
    public void initFieldParsers(ParserContext context) {
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
            // for user's requirement to parse itemid from referrer
            addFieldParser(new EnrichedItemIdParser());

            if (!context.get(CJS_PARSER_DISABLED, false)) {
                // CAUTION: This CJS parser has to be invoked at last
                addFieldParser(new CjsParser());
            } else {
                context.invokeTrigger(CJS_PARSER_DISABLED, null);
            }
        }
    }
}
