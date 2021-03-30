package com.ebay.sojourner.common.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlagsUtilsTest {

  @BeforeEach
  void setUp() {
  }

  @Test
  void test_isBitSet() {
    Assertions.assertThat(FlagsUtils.isBitSet("QA**", 0)).isFalse();
    Assertions.assertThat(FlagsUtils.isBitSet("QgA5IMIAACAAAAICQ0AAABAAgQAhAIAAAAzwAAg*", 0))
              .isFalse();
    Assertions.assertThat(FlagsUtils.isBitSet("QgA5IMIAACAAAAICQ0AAABAAgQAhAIAAAAzwAAg*", 1))
              .isTrue();
    Assertions.assertThat(FlagsUtils.isBitSet("QgA5IMIAACAAAAICQ0AAABAAgQAhAIAAAAzwAAg*", Integer.MAX_VALUE))
              .isFalse();
  }
}