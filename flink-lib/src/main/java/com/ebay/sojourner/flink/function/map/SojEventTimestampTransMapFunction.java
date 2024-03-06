package com.ebay.sojourner.flink.function.map;

import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

public class SojEventTimestampTransMapFunction extends RichMapFunction<SojEvent, SojEvent> {

  @Override
  public SojEvent map(SojEvent sojEvent) throws Exception {

    if (sojEvent.getEventTimestamp() != null) {
      sojEvent.setEventTimestamp(SojDateTimeUtils.toSojTs(sojEvent.getEventTimestamp()));
    }

    return sojEvent;
  }
}
