package com.ebay.sojourner.integration.source;


import com.ebay.sojourner.common.model.RheosHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;

@Slf4j
public class LocalSourceFunction implements SourceFunction<RheosHeader> {

    @Override
    public void run(SourceContext<RheosHeader> sourceContext) throws Exception {
        while (true) {
            LocalDateTime now = LocalDateTime.now();
            RheosHeader rheosHeader = new RheosHeader();
            rheosHeader.setEventId(now.toString());
            rheosHeader.setEventCreateTimestamp(System.currentTimeMillis());
            rheosHeader.setEventSentTimestamp(System.currentTimeMillis());
            rheosHeader.setSchemaId(0);
            rheosHeader.setProducerId("local-producer");

            sourceContext.collect(rheosHeader);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        log.info("LocalSourceFunction cancelled");
    }
}
