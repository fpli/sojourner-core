package com.ebay.sojourner.cjs.model;

import com.ebay.sojourner.cjs.service.JexlService;
import com.ebay.sojourner.common.model.RawEvent;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.Objects;

@NoArgsConstructor
public class JexlFieldExtractor extends FieldExtractor {

    @Override
    protected Object extractValue(RawEvent event) {

        val expression = JexlService.getExpression(formula);

        if (Objects.isNull(expression)) {
            return null;
        }

        val context = JexlService.getContext();
        context.clear();
        context.set("event", event);

        return expression.evaluate(context);
    }
}
