package com.ebay.sojourner.common.util;

/**
 * This is copied from JetStream to align 'cflags' filter logic
 */
public final class FlagsUtils {

  private FlagsUtils() {
    // Utility class
  }

  public static boolean isBitSet(String flgs, int position) {
    if (flgs != null && flgs.trim().length() > 0) {
      byte[] decodedBuckets = Base64Ebay.decode(flgs, false);
      int bucket = position / 8;
      if (bucket < decodedBuckets.length) {
        int actualFlag = decodedBuckets[bucket];
        int bitLocation = position % 8;
        int val = (actualFlag >> (7 - bitLocation)) & 1;
        return val == 1 ? true : false;
      }
    }
    return false;
  }
}
