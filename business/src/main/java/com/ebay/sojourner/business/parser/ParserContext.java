package com.ebay.sojourner.business.parser;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.ebay.sojourner.cjs.util.ConditionalInvoke.invokeIfNotNull;
import static com.ebay.sojourner.cjs.util.ConditionalMap.mapIfNotNull;

@ThreadSafe
public class ParserContext implements Serializable {

    private final Map<String, Supplier<Object>> configs;
    private final Map<String, Consumer<Object>> triggers;

    public ParserContext(Map<String, Supplier<Object>> configs,
                         Map<String, Consumer<Object>> triggers) {
        this.configs = configs;
        this.triggers = triggers;
    }

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("unchecked")
    public <T> T get(@NonNull String configName) {
        return (T) mapIfNotNull(configs.get(configName), Supplier::get);
    }

    public <T> T get(String configName, T fallback) {
        T value = get(configName);
        if (Objects.nonNull(value)) {
            return value;
        } else {
            return fallback;
        }
    }

    public void invokeTrigger(@NonNull String triggerName, Object param) {
        invokeIfNotNull(triggers.get(triggerName), x -> x.accept(param));
    }

    @NotThreadSafe
    public static class Builder {

        private final Map<String, Supplier<Object>> configs = new HashMap<>();
        private final Map<String, Consumer<Object>> triggers = new HashMap<>();

        public Builder() {
        }

        public Builder registerConfig(@NonNull String configName, @NonNull Supplier<Object> config) {
            configs.put(configName, config);
            return this;
        }

        public Builder registerTrigger(@NonNull String triggerName, @NonNull Consumer<Object> trigger) {
            triggers.put(triggerName, trigger);
            return this;
        }

        public ParserContext build() {
            return new ParserContext(ImmutableMap.copyOf(configs), ImmutableMap.copyOf(triggers));
        }
    }
}
