package com.ebay.sojourner.ubd.common.util;

import com.ebay.sojourner.ubd.common.model.UbiEvent;

public class FlagUtils {

  public static boolean matchFlag(UbiEvent event, int bitPosition, int expectedValue) {
    int result = SOJExtractFlag.extractFlag(event.getFlags(), bitPosition);
    return (result == expectedValue);
  }
}
