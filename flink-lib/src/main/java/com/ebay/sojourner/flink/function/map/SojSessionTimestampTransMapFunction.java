package com.ebay.sojourner.flink.function.map;

import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.util.SojDateTimeUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

public class SojSessionTimestampTransMapFunction extends RichMapFunction<SojSession, SojSession> {

  @Override
  public SojSession map(SojSession sojSession) throws Exception {

    if (sojSession.getSessionStartDt() != null) {
      sojSession.setSessionStartDt(SojDateTimeUtils.toSojTs(sojSession.getSessionStartDt()));
    }

    if (sojSession.getAbsStartTimestamp() != null) {
      sojSession.setAbsStartTimestamp(SojDateTimeUtils.toSojTs(sojSession.getAbsStartTimestamp()));
    }

    return sojSession;
  }
}
