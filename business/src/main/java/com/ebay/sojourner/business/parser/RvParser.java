package com.ebay.sojourner.business.parser;

import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.UbiEvent;
import com.ebay.sojourner.common.util.Base64Ebay;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class RvParser implements FieldParser<RawEvent, UbiEvent> {

  private static final Logger log = Logger.getLogger(RvParser.class);
  private static final String CFLGS = "cflgs";

  private static int isBitSet(byte[] decodedBuckets, int position) {
    int bucket = position / 8;
    if (bucket < decodedBuckets.length) {
      int actualFlag = decodedBuckets[bucket];
      int bitLocation = position % 8;
      int bitValue = actualFlag >> 7 - bitLocation & 1;
      return bitValue;
    }
    return 0;
  }

  @Override
  public void init() throws Exception {
  }

  public void parse(RawEvent event, UbiEvent ubiEvent) {
    Map<String, String> map = new HashMap<>();
    map.putAll(event.getSojA());
    map.putAll(event.getSojK());
    map.putAll(event.getSojC());
    String cflgs = null;
    if (StringUtils.isNotBlank(map.get(CFLGS))) {
      cflgs = map.get(CFLGS);
    }
    ubiEvent.setRv(false);
    if (StringUtils.isNotBlank(cflgs)) {
      try {
        byte[] flagBytes = Base64Ebay.decode(cflgs, false);
        int bit1 = isBitSet(flagBytes, 1);
        int bit2 = isBitSet(flagBytes, 2);
        int bit3 = isBitSet(flagBytes, 3);
        if (bit2 == 1 || bit3 == 1
            || bit1 == 0 && bit2 == 0 && bit3 == 0) {
          ubiEvent.setRv(true);
        }

      } catch (Exception e) {
        // log.debug("Parsing cflgs failed, format incorrect: " + cflgs);
      }
    } else {
      ubiEvent.setRv(true);
    }
  }
}
