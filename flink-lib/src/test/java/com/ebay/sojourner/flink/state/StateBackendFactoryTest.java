package com.ebay.sojourner.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.junit.jupiter.api.Test;

class StateBackendFactoryTest {

  @Test
  void getStateBackend_fs() {
    StateBackend hashmapStateBackend = StateBackendFactory.getStateBackend("HASHMAP");
    assertThat(hashmapStateBackend).isInstanceOf(HashMapStateBackend.class);
  }

  @Test
  void getStateBackend_rocksdb() {
    StateBackend rocksdbStateBackend = StateBackendFactory.getStateBackend("ROCKSDB");
    assertThat(rocksdbStateBackend).isInstanceOf(EmbeddedRocksDBStateBackend.class);
  }

  @Test
  void getStateBackend_error() {
    assertThrows(RuntimeException.class, () -> StateBackendFactory.getStateBackend("OTHER"));
  }
}