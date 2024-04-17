package com.ebay.sojourner.business.parser;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoding
 */
@Slf4j
public abstract class RecordParser<Source, Target> implements Parser<Source, Target> {

    protected List<FieldParser<Source, Target>> fieldParsers = new ArrayList<>();

    public abstract void initFieldParsers(ParserContext context);

    public void init(ParserContext context) throws Exception {
        for (FieldParser<Source, Target> parser : fieldParsers) {
            parser.init(context);
        }
    }


    public void parse(Source source, Target target)
            throws Exception {
        for (FieldParser<Source, Target> parser : fieldParsers) {
            try {
                parser.parse(source, target);
            } catch (Exception e) {
                log.warn("Error parsing field: [{}]", parser.getClass().getSimpleName(), e);
            }

        }
    }


    public void addFieldParser(FieldParser<Source, Target> parser) {
        if (!fieldParsers.contains(parser)) {
            fieldParsers.add(parser);
        } else {
            throw new RuntimeException("Duplicate Parser!!  ");
        }
    }

}
