package com.ebay.sojourner.integration.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.flink.api.common.functions.MapFunction;

@Slf4j
public class SimpleLogFunction<T> implements MapFunction<T, T> {

    @Override
    public T map(T value) throws Exception {
        log.info(ToStringBuilder.reflectionToString(value, ToStringStyle.JSON_STYLE));
        return value;
    }
}
