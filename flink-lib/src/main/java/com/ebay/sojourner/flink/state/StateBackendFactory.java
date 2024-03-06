package com.ebay.sojourner.flink.state;

import com.ebay.sojourner.common.util.Property;
import com.ebay.sojourner.flink.common.FlinkEnvUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

@Deprecated
@Slf4j
public class StateBackendFactory {

  public static final String HASHMAP = "HASHMAP";
  public static final String ROCKSDB = "ROCKSDB";
  public static final String CHECKPOINT_DATA_URI =
      "file://" + FlinkEnvUtils.getString(Property.CHECKPOINT_DATA_DIR);

  public static StateBackend getStateBackend(String type) {
    switch (type) {
      case HASHMAP:
        return new HashMapStateBackend();
      case ROCKSDB:
        try {
          EmbeddedRocksDBStateBackend rocksDBStateBackend =
              new EmbeddedRocksDBStateBackend();
          rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
          return rocksDBStateBackend;
        } catch (Exception e) {
          log.error("Failed to create RocksDB state backend", e);
          throw new RuntimeException(e);
        }
      default:
        throw new RuntimeException("Unknown state backend type");
    }
  }
}
