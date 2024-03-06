package com.ebay.sojourner.flink.connector.hdfs;

import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import java.util.UUID;

public class OutputFileConfigUtils {

    public static OutputFileConfig withRandomUUID() {

        return OutputFileConfig.builder()
                               .withPartPrefix("part-" + UUID.randomUUID())
                               .build();
    }

}
