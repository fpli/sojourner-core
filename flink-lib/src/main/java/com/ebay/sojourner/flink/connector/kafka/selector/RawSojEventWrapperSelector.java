package com.ebay.sojourner.flink.connector.kafka.selector;

import com.ebay.sojourner.common.model.RawSojEventWrapper;
import io.ebay.rheos.flink.connector.kafkaha.sink.TopicSelector;

public class RawSojEventWrapperSelector implements TopicSelector<RawSojEventWrapper> {
    @Override
    public String apply(RawSojEventWrapper rawSojEventWrapper) {
        return rawSojEventWrapper.getTopic();
    }
}
