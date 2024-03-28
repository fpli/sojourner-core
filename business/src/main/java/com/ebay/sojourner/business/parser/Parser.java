package com.ebay.sojourner.business.parser;

/**
 * @author kofeng
 */
public interface Parser<Source, Target> {

    default void init() throws Exception {

    }

    default void init(ParserContext context) throws Exception {
        init();
    }

    void parse(Source source, Target target) throws Exception;
}
