package com.ebay.sojourner.common.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IsValidGuidTest {
  @BeforeEach
  void setUp() {
  }

  @Test
  void test_validGuid() {
    Assertions.assertThat(IsValidGuid.isValidGuid("iisguide868cd4871b640298e7d9a73b")).isFalse();
    Assertions.assertThat(IsValidGuid.isValidGuid("7f75d8511800aa3262749557ffff2fd0")).isTrue();
  }
}
