package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.cjs.model.SignalDefinition;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.ebay.sojourner.cjs.util.ConditionalInvoke.invokeIfNotNull;

public class CjsFormulaInterpreterContext {

    public static final String CJS_TAG_NAME = "cjs";
    public static final String CJSBETA_TAG_NAME = "cjsBeta";
    public static final String CJSMOCK_TAG_NAME = "cjsMock";

    public static final String CJS_METRIC_PREFIX = "cjs.cjs.";
    public static final String CJSBETA_METRIC_PREFIX = "cjs.cjsBeta.";
    public static final String CJSMOCK_METRIC_PREFIX = "cjs.cjsMock.";

    public static final String MATCH_FILTER_METRIC = "match_filter";
    public static final String JEXL_EXCEPTION_METRIC = "jexl_exception";
    public static final String DEVICE_CTX_EXCEPTION_METRIC = "device_exception";
    public static final String NULL_POINTER_EXCEPTION_METRIC = "null_pointer_exception";

    public static final String INVOCATION_FAILURE = "invocation.failure";

    public static final String CJS_MATCH_FILTER_METRIC = CJS_METRIC_PREFIX + MATCH_FILTER_METRIC;
    public static final String CJS_JEXL_EXCEPTION_METRIC = CJS_METRIC_PREFIX + JEXL_EXCEPTION_METRIC;
    public static final String CJS_DEVICE_CTX_EXCEPTION_METRIC = CJS_METRIC_PREFIX + DEVICE_CTX_EXCEPTION_METRIC;
    public static final String CJS_NULL_POINTER_EXCEPTION_METRIC = CJS_METRIC_PREFIX + NULL_POINTER_EXCEPTION_METRIC;
    public static final String CJS_INVOCATION_FAILURE = CJS_METRIC_PREFIX + INVOCATION_FAILURE;

    public static final String CJSBETA_MATCH_FILTER_METRIC = CJSBETA_METRIC_PREFIX + MATCH_FILTER_METRIC;
    public static final String CJSBETA_JEXL_EXCEPTION_METRIC = CJSBETA_METRIC_PREFIX + JEXL_EXCEPTION_METRIC;
    public static final String CJSBETA_DEVICE_CTX_EXCEPTION_METRIC =
            CJSBETA_METRIC_PREFIX + DEVICE_CTX_EXCEPTION_METRIC;
    public static final String CJSBETA_NULL_POINTER_EXCEPTION_METRIC =
            CJSBETA_METRIC_PREFIX + NULL_POINTER_EXCEPTION_METRIC;
    public static final String CJSBETA_INVOCATION_FAILURE = CJSBETA_METRIC_PREFIX + INVOCATION_FAILURE;

    @NonNull
    @Getter
    private final Supplier<Map<String, SignalDefinition>> metadataProvider;

    @NonNull
    @Getter
    private final String tagName;

    @NonNull
    @Getter
    private final String metricPrefix;

    private final BiConsumer<String, Object> triggers;

    public CjsFormulaInterpreterContext(
            @NonNull String tagName,
            @NonNull Supplier<Map<String, SignalDefinition>> metadataProvider,
            @NonNull String metricPrefix,
            BiConsumer<String, Object> triggers) {

        this.tagName = tagName;
        this.metadataProvider = metadataProvider;

        this.metricPrefix = metricPrefix;

        if (Objects.isNull(triggers)) {
            triggers = (name, value) -> {
            };
        }
        this.triggers = triggers;
    }

    public void invokeTrigger(String triggerName, Object param) {
        invokeIfNotNull(triggers, x -> x.accept(triggerName, param));
    }

    public void invokeTriggerPrefixed(String triggerName, Object param) {
        invokeTrigger(getMetricPrefix() + triggerName, param);
    }
}
