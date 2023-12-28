package com.ebay.sojourner.flink.connector.kafka;

import com.ebay.sojourner.common.model.BotSignature;
import com.ebay.sojourner.common.model.JetStreamOutputEvent;
import com.ebay.sojourner.common.model.JetStreamOutputSession;
import com.ebay.sojourner.common.model.RawEvent;
import com.ebay.sojourner.common.model.SojEvent;
import com.ebay.sojourner.common.model.SojSession;
import com.ebay.sojourner.common.util.SojTimestamp;
import lombok.extern.slf4j.Slf4j;

@Deprecated
@Slf4j
public class TimestampFieldExtractor {

  public static <T> long getField(T t) {

    if (t instanceof RawEvent) {
      RawEvent rawEvent = (RawEvent) t;
      return SojTimestamp.getSojTimestampToUnixTimestamp(rawEvent.getEventTimestamp());
    } else if (t instanceof SojSession) {
      SojSession sojSession = (SojSession) t;
      try {
        return SojTimestamp.getSojTimestampToUnixTimestamp(sojSession.getAbsEndTimestamp());
      } catch (Exception e) {
        log.warn(
            "failed session record: " + sojSession.toString() + "; guid: " + sojSession.getGuid()
                + "; sessionskey: " + sojSession.getSessionSkey());
        log.warn("parse sessionstartdt failed: ", e);
        return System.currentTimeMillis();
      }

    } else if (t instanceof SojEvent) {
      SojEvent sojEvent = (SojEvent) t;
      return sojEvent.getEventTimestamp();
      //return SojTimestamp.getUnixTimestamp(sojEvent.getEventTimestamp());
    } else if (t instanceof JetStreamOutputEvent) {
      JetStreamOutputEvent jetStreamOutputEvent = (JetStreamOutputEvent) t;
      return jetStreamOutputEvent.getEventCreateTimestamp();
    } else if (t instanceof JetStreamOutputSession) {
      JetStreamOutputSession jetStreamOutputSession = (JetStreamOutputSession) t;
      return jetStreamOutputSession.getEventCreateTimestamp();
    } else if (t instanceof BotSignature) {
      BotSignature botSignature = (BotSignature) t;
      return botSignature.getExpirationTime();
    } else {
      throw new IllegalStateException("Cannot extract timestamp filed for generate watermark");
    }
  }
}
