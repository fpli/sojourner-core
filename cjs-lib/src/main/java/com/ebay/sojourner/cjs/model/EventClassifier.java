package com.ebay.sojourner.cjs.model;

import com.ebay.sojourner.cjs.service.JexlService;
import com.ebay.sojourner.cjs.util.SignalContext;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventClassifier implements Serializable {

    @NotNull
    private String type;

    @NotBlank
    private String name;

    @NotNull
    private EventSource source;

    @NotNull
    private String filter;

    public boolean match(SignalContext context) {

        if (isBlank(filter)) {
            return true; // Empty condition means pass through
        }

        val expression = JexlService.getExpression(filter);
        if (Objects.isNull(expression)) {
            return false; // Invalid expression should not pass through
        }

        try {
            return (boolean) expression.evaluate(context.getJexlContext());
        } catch (Exception ex) {
            return false;
        }
    }

}
