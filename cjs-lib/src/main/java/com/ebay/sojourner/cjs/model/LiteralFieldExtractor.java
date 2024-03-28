package com.ebay.sojourner.cjs.model;

import com.ebay.sojourner.common.model.RawEvent;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class LiteralFieldExtractor extends FieldExtractor {

    @Override
    protected Object extractValue(RawEvent event) {
        return formula;
    }
}
