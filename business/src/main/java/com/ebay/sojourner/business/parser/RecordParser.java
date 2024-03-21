package com.ebay.sojourner.business.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kofeng
 */
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
            parser.parse(source, target);
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
