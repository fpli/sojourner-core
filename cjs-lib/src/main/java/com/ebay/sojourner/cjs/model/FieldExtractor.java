package com.ebay.sojourner.cjs.model;

import com.ebay.sojourner.cjs.util.SignalContext;
import com.ebay.sojourner.common.model.RawEvent;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "clz"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = JexlFieldExtractor.class, name = "jexl"),
        @JsonSubTypes.Type(value = LiteralFieldExtractor.class, name = "lit")
})
@NoArgsConstructor
@Setter
@Slf4j
public abstract class FieldExtractor implements Serializable {

    @Getter
    private String type;

    @Getter
    private String name;

    protected String formula;

    protected abstract Object extractValue(RawEvent event);

    public SignalContext extractValue(@NotNull SignalContext context) {
        val event = context.getRawEvent();

        try {
            val value = extractValue(event);

            return context;
        } catch (Exception ex) {
            return null;
        }
    }
}
